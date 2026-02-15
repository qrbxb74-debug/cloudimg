import os
import io
import time
import uuid
import sqlite3
import random
import json
import difflib
import re
import threading
import logging
import gc
from flask import Flask, request, redirect, url_for, render_template, send_from_directory, jsonify, session, g, send_file, Response
from functools import wraps
from werkzeug.utils import secure_filename
import requests
from google_auth_oauthlib.flow import Flow
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from werkzeug.datastructures import FileStorage
from werkzeug.security import generate_password_hash
from werkzeug.middleware.proxy_fix import ProxyFix
from PIL import Image
from user_manager import UserManager
from visual import VisualRecognizer
from vertex_generator import VertexGenerator
from queue_manager import UploadQueueManager
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.config import Config

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Configure Logging to ensure output appears in Render logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def clean_asset_list(assets):
    """Ensures avatar paths in a list of asset dictionaries are just filenames."""
    for asset in assets:
        if asset.get('avatar') and isinstance(asset.get('avatar'), str) and asset['avatar'].startswith('/static/avatars/'):
            asset['avatar'] = asset['avatar'].replace('/static/avatars/', '')
    return assets

# Fix: Ensure the app runs from the script's directory to avoid path issues
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Database Configuration
# Use DB_FOLDER env var for persistence on Render Disks (e.g., /data)
DB_FOLDER = os.environ.get('DB_FOLDER', '.')

# Debug: Print the storage path to logs to verify persistence configuration
print("="*50)
print(f"STORAGE CONFIGURATION:")
print(f"DB_FOLDER Environment Variable: {os.environ.get('DB_FOLDER')}")
print(f"Active Storage Path: {os.path.abspath(DB_FOLDER)}")
print("="*50)

# Configuration
UPLOAD_FOLDER = os.path.join(DB_FOLDER, 'uploads')
TEMP_FOLDER = os.path.join(DB_FOLDER, 'temp_uploads')
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'svg', 'webp'}

app = Flask(__name__)
# Fix: Tell Flask it is behind a Proxy (Cloudflare/Render) so it generates HTTPS links
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload size
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'your_secure_secret_key_here')  # Required for session management

# Hardcoded Admin Emails - Add more here if needed
ADMIN_EMAILS = ['qrbxb70@gmail.com', 'qrbxb71@gmail.com']

# Google OAuth Configuration
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET')

# Vertex AI Image Generation Configuration
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_LOCATION = os.environ.get('GCP_LOCATION', 'us-central1')
# This is required for local development with HTTP. Remove in production.

# Google OAuth Flow Configuration must be defined before use.
PRODUCTION_URL = os.environ.get('PRODUCTION_URL', '').rstrip('/')

# Define all possible redirect URIs. The one used will be determined dynamically by url_for().
# This list is primarily for the client library's internal checks and for clarity.
# The critical configuration is in your Google Cloud Console.
redirect_uris = [
    "http://127.0.0.1:5000/login/google/callback"
]
if PRODUCTION_URL:
    redirect_uris.append(f"{PRODUCTION_URL}/login/google/callback")

client_secrets = {
    "web": {
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "redirect_uris": redirect_uris
    }
}

GOOGLE_SCOPES = ['openid', 'https://www.googleapis.com/auth/userinfo.email', 'https://www.googleapis.com/auth/userinfo.profile']

is_http_dev = False
try:
    if any(uri.startswith('http://') for uri in client_secrets['web']['redirect_uris']):
        is_http_dev = True
except (KeyError, TypeError):
    pass

if os.environ.get('FLASK_DEBUG', 'False').lower() == 'true' or is_http_dev:
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    print("INFO: OAUTHLIB_INSECURE_TRANSPORT enabled for local development over HTTP.")

USERS_DB = os.path.join(DB_FOLDER, 'users.db')
NOTIF_DB = os.path.join(DB_FOLDER, 'notifications.db')
REPORT_DB = os.path.join(DB_FOLDER, 'report.db')
PENDING_DB = os.path.join(DB_FOLDER, 'pending_uploads.db')
GENERATED_DB = os.path.join(DB_FOLDER, 'generated.db')
AVATAR_STORAGE_FOLDER = os.path.join(DB_FOLDER, 'profile_avatars')

# Ensure persistent directories exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(TEMP_FOLDER, exist_ok=True)
os.makedirs(DB_FOLDER, exist_ok=True)
os.makedirs(AVATAR_STORAGE_FOLDER, exist_ok=True)

# R2 / S3 Configuration
R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_BUCKET_NAME = os.environ.get('R2_BUCKET_NAME', 'cloudimg')

# Public Access URL (Enable "R2.dev subdomain" in Cloudflare Settings -> Public Access)
# Example: https://pub-123456789.r2.dev
R2_DOMAIN = os.environ.get('R2_DOMAIN', '').rstrip('/')

# Safety: Ignore the placeholder if the user hasn't updated .env yet
if 'YOUR-SUBDOMAIN-HERE' in R2_DOMAIN:
    R2_DOMAIN = ''

s3_client = None

if not (R2_ACCOUNT_ID and R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY):
    print("CRITICAL ERROR: R2 Credentials missing in .env file.")
    print("App will exit because 'R2 or Nothing' policy is active.")
    exit(1)

try:
    s3_client = boto3.client(
        's3',
        endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version='s3v4'),
        region_name='auto'
    )
    print("R2 Storage Initialized Successfully.")
except Exception as e:
    print(f"CRITICAL: Failed to connect to R2: {e}")
    exit(1)


def upload_to_r2(file_path, object_name):
    """Uploads a file to R2 and returns True if successful."""
    if not s3_client or not R2_BUCKET_NAME: return False
    try:
        # Determine content type
        content_type = 'application/octet-stream'
        lower_name = object_name.lower()
        if lower_name.endswith('.webp'): content_type = 'image/webp'
        elif lower_name.endswith('.jpg') or lower_name.endswith('.jpeg'): content_type = 'image/jpeg'
        elif lower_name.endswith('.png'): content_type = 'image/png'
        elif lower_name.endswith('.svg'): content_type = 'image/svg+xml'
        
        s3_client.upload_file(file_path, R2_BUCKET_NAME, object_name, ExtraArgs={'ContentType': content_type})
        return True
    except Exception as e:
        print(f"R2 Upload Error: {e}")
        return False

# Database Configuration for the 3 types
DB_MAPPING = {
    'image': os.path.join(DB_FOLDER, '1img.sql'),
    'logo': os.path.join(DB_FOLDER, '2logo.sql')
}

# Initialize Visual Recognizer
# Note: For production, it is safer to store this in an environment variable.

# Single API Key (Paid Tier)
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY')
visual_recognizer = VisualRecognizer(api_key=GOOGLE_API_KEY)

# Initialize Vertex AI Generator
vertex_generator = VertexGenerator(project_id=GCP_PROJECT_ID, location=GCP_LOCATION, temp_folder=TEMP_FOLDER)

# Initialize Queue Manager
# Rate limit: Reduced to 1.0s since we are on paid tier (much faster)
# Use persistent storage for the queue to prevent file loss on Render
QUEUE_STORAGE = os.path.join(DB_FOLDER, 'queue_storage')
queue_manager = UploadQueueManager(visual_recognizer, temp_storage_path=QUEUE_STORAGE, rate_limit_seconds=1.0, vertex_generator=vertex_generator)
# Do NOT start the worker in the global scope. It causes issues with Gunicorn's forking model.
# We will start it lazily on the first request to each worker process.

# Global Semaphore to limit concurrent image processing (Fix for 512MB RAM limit)
# Allowing only 1 concurrent heavy image process ensures we don't spike over memory limits.
processing_sem = threading.Semaphore(1)

# ===================================================================================
#                                 I18N (TRANSLATION) ENGINE
# ===================================================================================
SUPPORTED_LANGUAGES = ['en', 'fr', 'es', 'de', 'pt']
DEFAULT_LANGUAGE = 'en'
TRANSLATIONS = {}

def load_translations():
    """Loads JSON translation files from the 'messages' directory."""
    global TRANSLATIONS
    messages_dir = os.path.join(os.path.dirname(__file__), 'messages')
    os.makedirs(messages_dir, exist_ok=True)
    
    for lang in SUPPORTED_LANGUAGES:
        file_path = os.path.join(messages_dir, f'{lang}.json')
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    TRANSLATIONS[lang] = json.load(f)
            except Exception as e:
                print(f"Error loading {lang} translations: {e}")
                TRANSLATIONS[lang] = {}
        else:
            TRANSLATIONS[lang] = {}

# Initialize translations on startup
load_translations()

# Helper function for SSR (extracted from get_assets logic)
def get_ssr_assets(category, page=1, limit=20, user_id=0):
    offset = (page - 1) * limit
    try:
        conn = sqlite3.connect(DB_MAPPING[category])
        conn.row_factory = sqlite3.Row
        conn.execute(f"ATTACH DATABASE '{USERS_DB}' AS users_db")
        cursor = conn.cursor()
        
        sql = f'''
            SELECT t.id, t.user_id, 
            t.name, t.description, t.color_code, t.key_word, t.link_tiny,
            t.resolution, t.quality, t.category, t.link_small, t.link_medium, t.link_original, t.upload_date, t.likes, t.views, t.downloads, t.ai_data,
            '{category}' as category_type, users.username, users.avatar,
            EXISTS(SELECT 1 FROM users_db.user_likes WHERE user_id = ? AND asset_id = t.id AND category = ?) as is_liked,
            EXISTS(SELECT 1 FROM users_db.user_saves WHERE user_id = ? AND asset_id = t.id AND category = ?) as is_saved
            FROM uploads t
            LEFT JOIN users_db.users users ON t.user_id = users.id 
            ORDER BY upload_date DESC LIMIT ? OFFSET ?
        '''
        
        params = [user_id, category, user_id, category, limit, offset]
        cursor.execute(sql, params)
        assets = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return clean_asset_list(assets)
    except Exception as e:
        print(f"SSR Fetch Error: {e}")
        return []

# --- Worker Thread Initialization for Gunicorn ---
# This ensures that each Gunicorn worker process gets its own background thread.
_worker_started = False
_worker_lock = threading.Lock()

def start_background_worker():
    """Starts the queue manager's worker thread if not already started for this process."""
    global _worker_started
    with _worker_lock:
        # Check if thread is actually running, restart if dead
        if not _worker_started:
            logging.info("Flask App: Initializing background worker threads...")
            if not queue_manager.is_alive():
                queue_manager.start_worker()
            
            # Start Maintenance Thread (Runs every 30 minutes)
            def maintenance_loop():
                while True:
                    time.sleep(1800) # 30 minutes
                    cleanup_temp_files()
            threading.Thread(target=maintenance_loop, daemon=True).start()
            
            _worker_started = True
        elif not queue_manager.is_alive():
            logging.info("Flask App: Restarting dead queue worker...")
            queue_manager.start_worker()
# -----------------------------------------------

@app.before_request
def before_request():
    """Detects user language preference before every request."""
    # Start the background worker on the first request to this process.
    start_background_worker()

    # Fix: Clean avatar path in session if it has legacy prefix
    if 'avatar' in session and session['avatar'] and session['avatar'].startswith('/static/avatars/'):
        session['avatar'] = session['avatar'].replace('/static/avatars/', '')

    if 'lang' in session:
        g.lang = session['lang']
    else:
        # Auto-detect from browser headers
        g.lang = request.accept_languages.best_match(SUPPORTED_LANGUAGES) or DEFAULT_LANGUAGE

@app.after_request
def add_no_cache_headers(response):
    """
    Ensures that pages dependent on authentication state are not cached by the browser.
    This fixes a common issue where a user logs in but sees the old, cached "logged-out" page
    until they manually refresh.
    """
    if response.content_type.startswith('text/html'):
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
    return response

@app.context_processor
def inject_i18n():
    """Injects the 't' function into all HTML templates."""
    def t(key, default=None, **kwargs):
        # 1. Get dictionary for current language
        lang_data = TRANSLATIONS.get(g.get('lang', DEFAULT_LANGUAGE), {})
        # 2. Use provided default if key is not found, otherwise use key itself.
        default_value = default if default is not None else key
        text = lang_data.get(key, default_value)
        # 3. Format variables (e.g. "Hello {name}")
        return text.format(**kwargs) if kwargs else text
        
    return dict(t=t, _=t, current_lang=g.get('lang', DEFAULT_LANGUAGE))

@app.route('/set-language/<lang_code>')
def set_language(lang_code):
    """Route to switch language manually."""
    if lang_code in SUPPORTED_LANGUAGES:
        session['lang'] = lang_code
    return redirect(request.referrer or url_for('index'))

# Rate Limiting Storage
# Format: { 'ip_address': { 'blocked_until': timestamp, 'history': [(timestamp, count), ...] } }
rate_limit_store = {}

def init_databases():
    """Initializes the three separate SQLite databases."""
    # Define schemas for each category based on requirements
    schemas = {
        'image': '''
            CREATE TABLE IF NOT EXISTS uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                name TEXT,
                description TEXT,
                color_code TEXT,
                key_word TEXT,
                resolution TEXT,
                quality TEXT,
                category TEXT,
                link_small TEXT,
                link_medium TEXT,
                link_original TEXT,
                upload_date REAL,
                likes INTEGER DEFAULT 0,
                views INTEGER DEFAULT 0,
                downloads INTEGER DEFAULT 0,
                ai_data TEXT
            );
        ''',
        'logo': '''
            CREATE TABLE IF NOT EXISTS uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                name TEXT,
                description TEXT,
                color_code TEXT,
                key_word TEXT,
                category TEXT,
                link_small TEXT,
                link_medium TEXT,
                link_original TEXT,
                link_tiny TEXT,
                upload_date REAL,
                likes INTEGER DEFAULT 0,
                views INTEGER DEFAULT 0,
                downloads INTEGER DEFAULT 0,
                ai_data TEXT
            );
        '''
    }

    for category, db_name in DB_MAPPING.items():
        try:
            conn = sqlite3.connect(db_name)
            # Enable Write-Ahead Logging (WAL) for concurrency
            # This allows reading (searching) while writing (uploading)
            conn.execute("PRAGMA journal_mode=WAL;")
            cursor = conn.cursor()
            if category in schemas:
                cursor.executescript(schemas[category])
                
                # Migration: Ensure columns exist for existing databases
                new_columns = [
                    ("user_id", "INTEGER"),
                    ("description", "TEXT"),
                    ("likes", "INTEGER DEFAULT 0"),
                    ("views", "INTEGER DEFAULT 0"),
                    ("downloads", "INTEGER DEFAULT 0"),
                    ("category", "TEXT"),
                    ("ai_data", "TEXT"),
                    ("link_tiny", "TEXT")
                ]
                for col, dtype in new_columns:
                    try:
                        cursor.execute(f"ALTER TABLE uploads ADD COLUMN {col} {dtype}")
                    except sqlite3.OperationalError:
                        pass # Column likely exists
            
            conn.commit()
            conn.close()
            print(f"Database {db_name} initialized successfully.")
        except Exception as e:
            print(f"Error initializing database {db_name}: {e}")

init_databases()

def init_user_interactions():
    """Initializes the user interaction tables in users.db."""
    try:
        conn = sqlite3.connect(USERS_DB)
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_likes (
                user_id INTEGER,
                asset_id INTEGER,
                category TEXT,
                PRIMARY KEY (user_id, asset_id, category)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_saves (
                user_id INTEGER,
                asset_id INTEGER,
                category TEXT,
                PRIMARY KEY (user_id, asset_id, category)
            )
        ''')
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error initializing user interactions: {e}")

init_user_interactions()

def init_notification_db():
    """Initializes the notifications database."""
    try:
        conn = sqlite3.connect(NOTIF_DB)
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                message TEXT,
                type TEXT,
                created_at REAL,
                is_read INTEGER DEFAULT 0
            )
        ''')
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error initializing notifications db: {e}")

init_notification_db()

def init_preferences_db():
    """Initializes the user preferences table in users.db."""
    try:
        conn = sqlite3.connect(USERS_DB)
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_preferences (
                user_id INTEGER PRIMARY KEY,
                theme_color TEXT,
                theme_mode TEXT DEFAULT 'light'
            )
        ''')
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error initializing preferences db: {e}")

    # Migration for existing DBs
    try:
        conn = sqlite3.connect(USERS_DB)
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE user_preferences ADD COLUMN theme_mode TEXT DEFAULT 'light'")
        conn.commit()
        conn.close()
    except:
        pass

init_preferences_db()

def init_report_db():
    """Initializes the reports database."""
    try:
        conn = sqlite3.connect(REPORT_DB)
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                asset_id INTEGER,
                category TEXT,
                reasons TEXT,
                message TEXT,
                created_at REAL,
                status TEXT DEFAULT 'pending'
            )
        ''')
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error initializing report db: {e}")

init_report_db()

def init_pending_db():
    """Initializes the pending uploads database for admin persistence."""
    try:
        conn = sqlite3.connect(PENDING_DB)
        conn.execute('''CREATE TABLE IF NOT EXISTS pending (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE,
            original_name TEXT,
            r2_data TEXT,
            created_at REAL
        )''')
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error initializing pending db: {e}")

init_pending_db()

def init_generated_db():
    """Initializes the database for saved AI generations."""
    try:
        conn = sqlite3.connect(GENERATED_DB)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute('''
            CREATE TABLE IF NOT EXISTS user_generations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                filename TEXT,
                prompt TEXT,
                r2_key TEXT,
                created_at REAL
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS generation_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                prompt TEXT,
                status TEXT,
                created_at REAL
            )
        ''')
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error initializing generated db: {e}")

init_generated_db()

def fix_avatar_paths():
    """Migration: Removes '/static/avatars/' prefix from avatars in DB."""
    try:
        conn = sqlite3.connect(USERS_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT id, avatar FROM users WHERE avatar LIKE '/static/avatars/%'")
        rows = cursor.fetchall()
        if rows:
            for row in rows:
                user_id, avatar = row
                new_avatar = avatar.replace('/static/avatars/', '')
                cursor.execute("UPDATE users SET avatar = ? WHERE id = ?", (new_avatar, user_id))
            conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error fixing avatars: {e}")

fix_avatar_paths()

def create_notification(user_id, message, notif_type):
    """Helper to create a notification."""
    try:
        conn = sqlite3.connect(NOTIF_DB)
        conn.execute("INSERT INTO notifications (user_id, message, type, created_at) VALUES (?, ?, ?, ?)", 
                     (user_id, message, notif_type, time.time()))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error creating notification: {e}")

# Initialize User Manager
user_manager = UserManager(db_name=USERS_DB)

# Avatar Library Configuration
AVATAR_LIBRARY = [f'cartoon{i}.jpg' for i in range(1, 18)]  # Assumes cartoon1.jpg to cartoon17.jpg exist in static/avatars/

# Global Cache for Analysis Data
TEMP_ANALYSIS_FILE = os.path.join(DB_FOLDER, 'analysis_cache.json')
analysis_cache = {}

def load_analysis_cache():
    """Loads the analysis cache from disk into memory."""
    global analysis_cache
    if os.path.exists(TEMP_ANALYSIS_FILE):
        try:
            with open(TEMP_ANALYSIS_FILE, 'r', encoding='utf-8') as f:
                analysis_cache = json.load(f)
        except Exception:
            analysis_cache = {}

def save_analysis_cache():
    """Saves the memory cache to disk."""
    try:
        with open(TEMP_ANALYSIS_FILE, 'w', encoding='utf-8') as f:
            json.dump(analysis_cache, f)
    except Exception as e:
        print(f"Error saving analysis cache: {e}")

# Initialize cache on startup
load_analysis_cache()

def cleanup_temp_files():
    """Deletes files in temp folders older than 30 minutes and syncs cache."""
    try:
        logging.info("MAINTENANCE: Starting cleanup of temp files and RAM...")
        now = time.time()
        max_age = 1800  # 30 Minutes (Sensitive cleanup)

        # 1. Clean temp_uploads (User Uploads)
        for filename in os.listdir(TEMP_FOLDER):
            # Protect the cache file
            if filename == 'analysis_cache.json':
                continue
                
            file_path = os.path.join(TEMP_FOLDER, filename)
            if os.path.isfile(file_path) and os.stat(file_path).st_mtime < now - max_age:
                try: os.remove(file_path)
                except: pass
        
        # 2. Clean temp_queue_storage (Queue Copies)
        if hasattr(queue_manager, 'temp_storage_path') and os.path.exists(queue_manager.temp_storage_path):
            for filename in os.listdir(queue_manager.temp_storage_path):
                file_path = os.path.join(queue_manager.temp_storage_path, filename)
                if os.path.isfile(file_path) and os.stat(file_path).st_mtime < now - max_age:
                    try: os.remove(file_path)
                    except: pass
        
        # 3. Sync cache (remove entries for missing files)
        keys_to_remove = [k for k in analysis_cache if not os.path.exists(os.path.join(TEMP_FOLDER, k))]
        
        if keys_to_remove:
            for k in keys_to_remove:
                del analysis_cache[k]
            save_analysis_cache()
            
        # Force Garbage Collection to clear RAM
        gc.collect()
        logging.info("MAINTENANCE: Cleanup complete. RAM cleared.")
            
    except Exception as e:
        logging.error(f"MAINTENANCE ERROR: {e}")

def allowed_file(filename):
    """Checks if the file's extension is allowed."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    """Renders the home page."""
    # Check for search query in URL (Deep Linking Support)
    query = request.args.get('q', '').strip()
    if query:
        return redirect(url_for('search_page', **request.args))

    if 'username' in session:
        print(f"DEBUG: User {session['username']} is logged in.")
    
    # Pagination Logic
    page = request.args.get('page', 1, type=int)
    limit = 20
    user_id = session.get('user_id', session.get('guest_id', 0))
    seo_assets = get_ssr_assets('image', page, limit, user_id)

    has_unread = False
    theme_color = '#0a84ff'
    theme_mode = 'light'

    if 'user_id' in session:
        try:
            conn = sqlite3.connect(NOTIF_DB)
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM notifications WHERE user_id = ? AND is_read = 0 LIMIT 1", (session['user_id'],))
            if cursor.fetchone():
                has_unread = True
            conn.close()

            # Fetch User Preferences
            if 'theme_color' in session:
                theme_color = session['theme_color']
            
            # Always try to fetch fresh prefs to get filters
            conn = sqlite3.connect(USERS_DB)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Refresh Role and Email from DB
            cursor.execute("SELECT role, email FROM users WHERE id = ?", (session['user_id'],))
            role_row = cursor.fetchone()
            if role_row: 
                session['role'] = role_row['role']
                session['email'] = role_row['email']

            cursor.execute("SELECT theme_color, theme_mode FROM user_preferences WHERE user_id = ?", (session['user_id'],))
            row = cursor.fetchone()
            if row:
                if row['theme_color']: 
                    theme_color = row['theme_color']
                    session['theme_color'] = theme_color
                if row['theme_mode']:
                    theme_mode = row['theme_mode']
            conn.close()
        except Exception as e:
            print(f"Error checking notifications: {e}")
            
    content = render_template('home.html', 
                           has_unread=has_unread, 
                           theme_color=theme_color, 
                           theme_mode=theme_mode, 
                           r2_domain=R2_DOMAIN,
                           seo_assets=seo_assets,
                           current_page=page)
                           
    # Debug: Print the size of the HTML to the terminal
    print(f"DEBUG: Sending home.html - Size: {len(content)} characters")
    return content

@app.route('/upload-page')
def upload_page():
    """Renders the upload page."""
    return render_template('index.html')

@app.route('/uploader-template')
def uploader_template():
    """Returns the uploader HTML template for dynamic loading."""
    allow_category = False
    if 'user_id' in session:
        try:
            with sqlite3.connect(USERS_DB) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT email FROM users WHERE id = ?", (session['user_id'],))
                row = cursor.fetchone()
                if row and row[0] in ADMIN_EMAILS:
                    allow_category = True
        except Exception as e:
            print(f"Error checking user permissions: {e}")
    return render_template('uploader.html', allow_category=allow_category)

@app.route('/image-editor-template')
def image_editor_template():
    """Returns the image editor HTML template for dynamic loading."""
    return render_template('image_editor.html', r2_domain=R2_DOMAIN)

@app.route('/image-generator-template')
def image_generator_template():
    """Returns the image generator HTML template for dynamic loading."""
    return render_template('image_generator.html')

@app.route('/settings-template')
def settings_template():
    """Returns the settings HTML template for dynamic loading."""
    user_data = {
        'username': '',
        'email': '',
        'avatar': None,
        'bio': '',
        'website': '',
        'instagram': '',
        'twitter': '',
        'contact_email': ''
    }
    
    if 'user_id' in session:
        try:
            conn = sqlite3.connect(USERS_DB)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Ensure bio column exists (Migration)
            try:
                cursor.execute("SELECT bio FROM users LIMIT 1")
            except sqlite3.OperationalError:
                cursor.execute("ALTER TABLE users ADD COLUMN bio TEXT")
                conn.commit()

            try:
                cursor.execute("SELECT website FROM users LIMIT 1")
            except sqlite3.OperationalError:
                cursor.execute("ALTER TABLE users ADD COLUMN website TEXT")
                cursor.execute("ALTER TABLE users ADD COLUMN instagram TEXT")
                cursor.execute("ALTER TABLE users ADD COLUMN twitter TEXT")
                cursor.execute("ALTER TABLE users ADD COLUMN contact_email TEXT")
                conn.commit()
                
            cursor.execute("SELECT username, email, avatar, bio, website, instagram, twitter, contact_email FROM users WHERE id = ?", (session['user_id'],))
            row = cursor.fetchone()
            if row:
                user_data = dict(row)
            conn.close()
        except Exception as e:
            print(f"Error fetching user settings: {e}")
            
    return render_template('settings.html', **user_data)

@app.route('/api/analyze', methods=['POST'])
def analyze_image():
    """Adds an image to the analysis queue."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'AUTH_REQUIRED'}), 401

    data = request.get_json()
    filename = data.get('filename')

    if not filename:
        return jsonify({'success': False, 'message': 'FILENAME_REQUIRED'}), 400

    # Construct path to the temp file (Sanitize filename for security)
    filename = secure_filename(filename)
    file_path = os.path.join(TEMP_FOLDER, filename)
    
    # Fallback: If original file was deleted by /upload (R2 optimization), try the thumbnail
    if not os.path.exists(file_path):
        thumb_path = os.path.join(TEMP_FOLDER, f"api_thumb_{filename}")
        if os.path.exists(thumb_path):
            file_path = thumb_path

    # Security check: ensure file is actually in temp folder
    if not os.path.exists(file_path) or not os.path.isfile(file_path):
        return jsonify({'success': False, 'message': 'FILE_NOT_FOUND'}), 404

    # Check for cached analysis to save tokens
    if filename in analysis_cache:
        data = analysis_cache[filename]
        print(f"DEBUG: Cache hit for {filename} - Serving saved analysis (0 Tokens Used)")
        return jsonify({'success': True, 'data': data})

    # Add to Queue instead of processing immediately
    task_id = queue_manager.add_to_queue('analyze', session['user_id'], source_file_path=file_path)
    
    # Check for high traffic (more than 2 items pending)
    response = {'success': True, 'task_id': task_id, 'status': 'queued'}
    try:
        if queue_manager.queue.qsize() > 2:
            response['high_traffic'] = True
            response['message'] = "HIGH_TRAFFIC_WARNING"
    except NotImplementedError:
        pass
        
    return jsonify(response)

@app.route('/api/generate_image', methods=['POST'])
def generate_image_route():
    """Adds an image generation request to the queue."""
    # Allow guest generation by assigning a temporary ID
    user_id = session.get('user_id')
    if not user_id:
        if 'guest_id' not in session:
            session['guest_id'] = -random.randint(100000, 999999999)
        user_id = session['guest_id']

    data = request.get_json()
    prompt = data.get('prompt')
    aspect_ratio = data.get('aspect_ratio', '1:1')

    if not prompt:
        return jsonify({'success': False, 'message': 'Prompt is required'}), 400

    # Log the request for admin stats
    try:
        with sqlite3.connect(GENERATED_DB) as conn:
            conn.execute("INSERT INTO generation_requests (user_id, prompt, status, created_at) VALUES (?, ?, ?, ?)", 
                         (user_id, prompt, 'queued', time.time()))
    except Exception as e:
        print(f"Error logging generation request: {e}")

    # Add to Queue for background processing
    task_id = queue_manager.add_to_queue(
        'generate',
        user_id,
        prompt=prompt,
        aspect_ratio=aspect_ratio
    )
    return jsonify({'success': True, 'task_id': task_id, 'status': 'queued'})

@app.route('/api/check_task/<task_id>')
def check_task(task_id):
    """Checks the status of a background analysis task."""
    task = queue_manager.get_task_status(task_id)
    if not task:
        return jsonify({'success': False, 'message': 'Task not found'}), 404
    
    if task['status'] == 'completed':
        response_data = {'success': True, 'status': 'completed', 'data': task['result']}
        # For generation tasks, include the task type so the frontend knows how to handle it
        if task.get('type') == 'generate':
            response_data['task_type'] = 'generate'
        return jsonify(response_data)
    elif task['status'] == 'failed':
        # Cleanup on ANY failure (API error, Safety flag, etc.)
        try:
            # Attempt to retrieve file path from task object
            file_path = task.get('file_path')
            
            if file_path:
                # 1. Delete original temp file
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass
                
                # 2. Delete API thumbnail
                directory, filename = os.path.split(file_path)
                thumb_path = os.path.join(directory, f"api_thumb_{filename}")
                if os.path.exists(thumb_path):
                    try:
                        os.remove(thumb_path)
                    except Exception:
                        pass

                # 3. Delete queue copy
                try:
                    queue_folder = queue_manager.temp_storage_path
                    if os.path.exists(queue_folder):
                        for q_file in os.listdir(queue_folder):
                            if q_file.endswith(f"_{filename}"):
                                try:
                                    os.remove(os.path.join(queue_folder, q_file))
                                except Exception:
                                    pass
                except Exception:
                    pass
        except Exception as e:
            print(f"Error cleaning up failed file: {e}")

        return jsonify({'success': False, 'status': 'failed', 'error': task.get('error'), 'critical_stop': task.get('critical_stop')})
    else:
        return jsonify({'success': True, 'status': task['status']}) # queued or processing

@app.route('/api/pending_tasks')
def get_pending_tasks():
    """Retrieves tasks that are processing or completed but not yet published."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Authentication required'}), 401
    tasks = queue_manager.get_tasks_for_user(session['user_id'])
    return jsonify({'success': True, 'tasks': tasks})

def process_and_save_image(src_path, dest_folder, filename, mode='image'):
    """
    Helper to process an image: resize, create thumbnails, and move original.
    Returns a dictionary with file links and metadata.
    """
    res = {}
    base_name = os.path.splitext(filename)[0]
    extension = filename.rsplit('.', 1)[1].lower()
    
    res['link_original'] = filename
    dst_original = os.path.join(dest_folder, filename)

    # Vector files (SVG)
    if extension == 'svg':
        res['link_small'] = filename
        res['link_medium'] = filename
        res['resolution'] = 'Vector'
        res['quality'] = 'SVG'
        os.rename(src_path, dst_original)
        return res

    try:
        with Image.open(src_path) as img:
            res['resolution'] = f"{img.size[0]}x{img.size[1]}"
            
            # Determine Quality
            long_side = max(img.size)
            if long_side >= 3840: res['quality'] = '4K'
            elif long_side >= 2560: res['quality'] = '2K'
            elif long_side >= 1920: res['quality'] = 'FHD'
            elif long_side >= 1280: res['quality'] = 'HD'
            else: res['quality'] = 'SD'

            # Processing based on mode
            if mode == 'image':
                rgb_im = img.convert('RGB')
                # Medium (WebP) - High quality for display
                rgb_im.thumbnail((2048, 2048))
                res['link_medium'] = 'medium_' + base_name + '.webp'
                rgb_im.save(os.path.join(dest_folder, res['link_medium']), 'WEBP', quality=90)
                
                # Small (WebP) - Optimized for thumbnails
                rgb_im.thumbnail((1024, 1024))
                res['link_small'] = 'small_' + base_name + '.webp'
                rgb_im.save(os.path.join(dest_folder, res['link_small']), 'WEBP', quality=60)
            else: # logo
                # Medium (WebP) - Process larger first to maintain quality
                img.thumbnail((1024, 1024))
                res['link_medium'] = 'medium_' + base_name + '.webp'
                img.save(os.path.join(dest_folder, res['link_medium']), 'WEBP', quality=95)
                
                # Small (WebP)
                img.thumbnail((512, 512))
                res['link_small'] = 'small_' + base_name + '.webp'
                img.save(os.path.join(dest_folder, res['link_small']), 'WEBP', quality=95)
        os.rename(src_path, dst_original)
        return res
    except Exception as e:
        print(f"Error processing {filename}: {e}")
        return None

def prepare_images_for_r2(src_path, base_filename):
    """
    Generates medium and small versions of the image at src_path.
    Returns a dict with paths to all 3 versions (original, medium, small) and metadata.
    """
    directory = os.path.dirname(src_path)
    ext = os.path.splitext(src_path)[1].lower()
    base_name_no_ext = os.path.splitext(base_filename)[0]
    
    paths = {
        'original': src_path,
        'medium': None,
        'small': None,
        'filename_original': base_filename,
        'filename_medium': None,
        'filename_small': None,
        'resolution': '',
        'quality': 'SD'
    }

    if ext == '.svg':
        paths['filename_medium'] = base_filename
        paths['filename_small'] = base_filename
        paths['medium'] = src_path
        paths['small'] = src_path
        paths['resolution'] = 'Vector'
        paths['quality'] = 'SVG'
        return paths

    def compress_and_save(image, path, target_kb):
        """Compresses image to target size in KB."""
        quality = 90
        step = 5
        min_quality = 5
        
        image.save(path, 'WEBP', quality=quality)
        while os.path.getsize(path) > target_kb * 1024 and quality > min_quality:
            quality -= step
            image.save(path, 'WEBP', quality=quality)

    try:
        rgb_im = None
        with Image.open(src_path) as img:
            paths['resolution'] = f"{img.size[0]}x{img.size[1]}"
            long_side = max(img.size)
            if long_side >= 3840: paths['quality'] = '4K'
            elif long_side >= 2560: paths['quality'] = '2K'
            elif long_side >= 1920: paths['quality'] = 'FHD'
            elif long_side >= 1280: paths['quality'] = 'HD'
            
            # MEMORY OPTIMIZATION: Resize BEFORE converting to RGB to save RAM.
            # We only generate Medium (2048px) and smaller. We do NOT need full resolution in RAM.
            if long_side > 2048:
                # Use draft for JPEGs (Fast & Low RAM)
                if img.format == 'JPEG':
                    try: img.draft('RGB', (2048, 2048))
                    except: pass
                # Force resize the object IN PLACE
                img.thumbnail((2048, 2048), Image.Resampling.LANCZOS)

            rgb_im = img.convert('RGB')
            
        # Medium
        medium_name = f"medium_{base_name_no_ext}.webp"
        medium_path = os.path.join(directory, medium_name)
        rgb_im.thumbnail((2048, 2048))
        compress_and_save(rgb_im, medium_path, 100)
        paths['medium'] = medium_path
        paths['filename_medium'] = medium_name
        
        # Small
        small_name = f"small_{base_name_no_ext}.webp"
        small_path = os.path.join(directory, small_name)
        rgb_im.thumbnail((1024, 1024))
        compress_and_save(rgb_im, small_path, 40)
        paths['small'] = small_path
        paths['filename_small'] = small_name

        # Tiny (Mobile Optimized)
        tiny_name = f"tiny_{base_name_no_ext}.webp"
        tiny_path = os.path.join(directory, tiny_name)
        rgb_im.thumbnail((400, 400))
        compress_and_save(rgb_im, tiny_path, 20)
        paths['tiny'] = tiny_path
        paths['filename_tiny'] = tiny_name
        
        # Cleanup RAM immediately
        del rgb_im
        gc.collect()
            
    except Exception as e:
        print(f"Error preparing images: {e}")
        return None
    return paths

# ===================================================================================
#                                 UPLOADS SECTION
# ===================================================================================
# WARNING: DO NOT MODIFY THE CODE IN THIS SECTION WITHOUT AUTHORIZATION.
# This includes file validation, temporary storage, publishing, and image compression.
# ===================================================================================
@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles the file upload process."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'AUTH_REQUIRED'}), 401

    # --- NEW: Handle Client-Side Processed Uploads (High Res) ---
    if request.form.get('client_processed') == 'true':
        try:
            # Retrieve files
            f_original = request.files.get('file_original')
            f_medium = request.files.get('file_medium')
            f_small = request.files.get('file_small')
            f_tiny = request.files.get('file_tiny')

            if not f_original:
                return jsonify({'success': False, 'message': 'UPLOAD_NO_FILE_PART'}), 400

            # Generate base filename
            extension = f_original.filename.rsplit('.', 1)[1].lower() if '.' in f_original.filename else 'jpg'
            safe_name = secure_filename(f_original.filename.rsplit('.', 1)[0])
            base_filename = f"{safe_name}-{int(time.time())}.{extension}"
            base_name_no_ext = os.path.splitext(base_filename)[0]

            # Define R2 filenames
            names = {
                'original': base_filename,
                'medium': f"medium_{base_name_no_ext}.webp",
                'small': f"small_{base_name_no_ext}.webp",
                'tiny': f"tiny_{base_name_no_ext}.webp"
            }

            # Helper to save and upload
            def save_and_upload(file_obj, r2_name):
                if not file_obj: return None
                temp_path = os.path.join(TEMP_FOLDER, r2_name)
                file_obj.save(temp_path)
                if s3_client:
                    upload_to_r2(temp_path, r2_name)
                return temp_path

            # Process uploads
            path_original = save_and_upload(f_original, names['original'])
            path_medium = save_and_upload(f_medium, names['medium'])
            path_small = save_and_upload(f_small, names['small'])
            path_tiny = save_and_upload(f_tiny, names['tiny'])

            # Create API Thumbnail (Copy small version for analysis)
            # This ensures /api/analyze has a file to work with without re-processing
            if path_small and os.path.exists(path_small):
                import shutil
                shutil.copy(path_small, os.path.join(TEMP_FOLDER, f"api_thumb_{base_filename}"))

            # Cleanup R2 temp files (keep api_thumb)
            for p in [path_original, path_medium, path_small, path_tiny]:
                if p and os.path.exists(p):
                    try: os.remove(p)
                    except: pass

            # Construct response data matching prepare_images_for_r2 format
            prep_res = {
                'filename_original': names['original'],
                'filename_medium': names['medium'],
                'filename_small': names['small'],
                'filename_tiny': names['tiny'],
                'resolution': request.form.get('resolution', 'Unknown'),
                'quality': request.form.get('quality', 'HD')
            }
            
            # Save to Pending DB
            try:
                with sqlite3.connect(PENDING_DB) as conn:
                    conn.execute("INSERT OR REPLACE INTO pending (filename, original_name, r2_data, created_at) VALUES (?, ?, ?, ?)", 
                                 (base_filename, f_original.filename, json.dumps(prep_res), time.time()))
            except Exception as e:
                print(f"Pending DB Error: {e}")

            return jsonify({'success': True, 'message': 'File uploaded to R2 (Client Processed).', 'filename': base_filename, 'r2_data': prep_res}), 200

        except Exception as e:
            print(f"Client Upload Error: {e}")
            return jsonify({'success': False, 'message': 'FILE_SAVE_FAILED'}), 500

    # --- EXISTING LOGIC (Legacy/Small Files) ---
    # Optimization: Only run cleanup 10% of the time to reduce I/O overhead
    if random.random() < 0.1:
        cleanup_temp_files()

    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'UPLOAD_NO_FILE_PART'}), 400
    
    file: FileStorage = request.files['file']

    if file.filename == '':
        return jsonify({'success': False, 'message': 'UPLOAD_NO_SELECTED_FILE'}), 400

    if file and allowed_file(file.filename):
        extension = file.filename.rsplit('.', 1)[1].lower()
        # Create a clean, timestamped filename for R2
        safe_name = secure_filename(file.filename.rsplit('.', 1)[0])
        filename = f"{safe_name}-{int(time.time())}.{extension}"
        file_path = os.path.join(TEMP_FOLDER, filename)

        try:
            # Save to temporary folder first
            file.save(file_path)

            # --- CRITICAL MEMORY FIX: SERIALIZE PROCESSING ---
            # We use a semaphore to ensure only 1 heavy image processing task runs at a time.
            # This prevents OOM errors on 512MB instances when multiple uploads happen.
            if not processing_sem.acquire(blocking=True, timeout=30):
                return jsonify({'success': False, 'message': 'SERVER_BUSY'}), 503
            
            try:
                # AI Validation: Ensure file is valid and recognized by Gemini
                # We skip SVG as it is a vector format not typically handled by Vision models in this context
                if extension != 'svg':
                    # Create a compressed version for the API to reduce token usage and latency
                    api_thumb_path = os.path.join(TEMP_FOLDER, f"api_thumb_{filename}")
                    
                    try:
                        with Image.open(file_path) as img:
                            # MEMORY FIX: Resize BEFORE converting to RGB
                            img.thumbnail((1024, 1024))
                            if img.mode in ('RGBA', 'P'):
                                img = img.convert('RGB')
                            img.save(api_thumb_path, 'JPEG', quality=60)
                    except Exception as e:
                        print(f"Error creating API thumbnail: {e}")

                # Note: We no longer analyze immediately here. 
                # We just confirm the upload. Analysis happens in /api/analyze via queue.
                
                # --- NEW LOGIC: DIRECT R2 UPLOAD ---
                # Process images (Resize)
                prep_res = prepare_images_for_r2(file_path, filename)
            finally:
                processing_sem.release()
                gc.collect()

            if not prep_res:
                return jsonify({'success': False, 'message': 'IMAGE_PROCESSING_FAILED'}), 500

            # Upload all versions to R2
            if s3_client:
                upload_to_r2(prep_res['original'], prep_res['filename_original'])
                if prep_res['medium'] and prep_res['medium'] != prep_res['original']:
                    upload_to_r2(prep_res['medium'], prep_res['filename_medium'])
                if prep_res['small'] and prep_res['small'] != prep_res['original']:
                    upload_to_r2(prep_res['small'], prep_res['filename_small'])
                if prep_res.get('tiny'):
                    upload_to_r2(prep_res['tiny'], prep_res['filename_tiny'])
            
            # Cleanup local temp files immediately
            if os.path.exists(file_path): os.remove(file_path)
            if prep_res['medium'] and os.path.exists(prep_res['medium']): os.remove(prep_res['medium'])
            if prep_res['small'] and os.path.exists(prep_res['small']): os.remove(prep_res['small'])
            if prep_res.get('tiny') and os.path.exists(prep_res['tiny']): os.remove(prep_res['tiny'])

            # Force garbage collection to free up RAM immediately
            gc.collect()

            # Save to Pending DB for persistence
            try:
                with sqlite3.connect(PENDING_DB) as conn:
                    conn.execute("INSERT OR REPLACE INTO pending (filename, original_name, r2_data, created_at) VALUES (?, ?, ?, ?)", 
                                 (filename, file.filename, json.dumps(prep_res), time.time()))
            except Exception as e:
                print(f"Pending DB Error: {e}")

            # Return R2 keys and metadata
            return jsonify({'success': True, 'message': 'File uploaded to R2.', 'filename': filename, 'r2_data': prep_res}), 200
        except Exception as e:
            if os.path.exists(file_path):
                os.remove(file_path)
            return jsonify({'success': False, 'message': 'FILE_SAVE_FAILED'}), 500
    
    return jsonify({'success': False, 'message': 'UPLOAD_FILE_TYPE_NOT_ALLOWED'}), 400

# -------------------------------------------------------------------
# Publish Route: Handles confirmation, compression, and final storage
# -------------------------------------------------------------------
@app.route('/publish', methods=['POST'])
def publish_file():
    """Moves a file from temp to confirmed uploads, processes based on type, and saves to specific DB."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'AUTH_REQUIRED'}), 401

    data = request.get_json()
    category = data.get('category', 'image') # Default to image, options: image, logo
    
    # Support bulk publishing
    filenames = data.get('filenames', [])
    r2_data_list = data.get('r2_data_list', []) # List of prep_res objects
    if not filenames and data.get('filename'):
        filenames = [data.get('filename')]

    # Extract metadata from request (Global overrides)
    req_name = data.get('name', '')
    req_color = data.get('color') or data.get('color_code') or data.get('color-code') or ''
    req_keywords = data.get('key_word') or data.get('keywords') or data.get('key word') or ''
    req_content_category = data.get('content_category')
    req_description = data.get('description', '')
    req_ai_data = data.get('ai_data')

    if category not in DB_MAPPING:
        return jsonify({'success': False, 'message': 'INVALID_CATEGORY'}), 400

    # Reload cache to ensure we have the latest analysis data from the worker
    load_analysis_cache()

    # =================================================================================
    # HANDLE STANDARD / BULK PUBLISHING
    # =================================================================================
    if not filenames:
        return jsonify({'success': False, 'message': 'PUBLISH_NO_FILES'}), 400

    conn = sqlite3.connect(DB_MAPPING[category])
    cursor = conn.cursor()
    
    success_count = 0
    errors = []

    # Handle single file publish with r2_data passed from frontend
    if not r2_data_list and data.get('r2_data'):
        r2_data_list = [data.get('r2_data')]

    # If we have r2_data, we use that (New Flow). If not, we fallback to filenames (Old Flow/Bulk Tool)
    # But since we updated /upload, filenames usually won't exist in temp anymore.
    
    loop_source = r2_data_list if r2_data_list else filenames

    for item in loop_source:
        fname = item['filename_original'] if isinstance(item, dict) else item
        
        # Check if we have R2 data directly
        if isinstance(item, dict):
            try:
                # --- FIX: Handle Generated Images (Local Temp -> R2) ---
                temp_path = os.path.join(TEMP_FOLDER, fname)
                
                # Check if we need to process this file (e.g. it's a generated image in temp)
                if os.path.exists(temp_path):
                    
                    # 1. Ensure Resolution/Quality
                    if 'resolution' not in item or 'quality' not in item:
                        try:
                            with Image.open(temp_path) as img:
                                item['resolution'] = f"{img.width}x{img.height}"
                                long_side = max(img.width, img.height)
                                if long_side >= 3840: item['quality'] = '4K'
                                elif long_side >= 2560: item['quality'] = '2K'
                                elif long_side >= 1920: item['quality'] = 'FHD'
                                elif long_side >= 1280: item['quality'] = 'HD'
                                else: item['quality'] = 'SD'
                        except Exception:
                            item['resolution'] = '1024x1024'
                            item['quality'] = 'HD'

                    # 2. Generate Variants & Upload to R2 (if not already done)
                    # If filename_medium is missing or same as original, we assume it needs processing
                    if (not item.get('filename_medium') or item.get('filename_medium') == fname) and s3_client:
                        print(f"DEBUG: Processing generated image for R2: {fname}")
                        prep_res = prepare_images_for_r2(temp_path, fname)
                        if prep_res:
                            upload_to_r2(prep_res['original'], prep_res['filename_original'])
                            if prep_res['medium']: upload_to_r2(prep_res['medium'], prep_res['filename_medium'])
                            if prep_res['small']: upload_to_r2(prep_res['small'], prep_res['filename_small'])
                            if prep_res.get('tiny'): upload_to_r2(prep_res['tiny'], prep_res['filename_tiny'])
                            
                            # Update item with new filenames
                            item.update(prep_res)
                            
                            # Cleanup variants
                            for k in ['medium', 'small', 'tiny']:
                                p = prep_res.get(k)
                                if p and p != prep_res['original'] and os.path.exists(p):
                                    try: os.remove(p)
                                    except: pass
                
                # Fallback for missing keys to prevent 500
                if 'filename_small' not in item: item['filename_small'] = fname
                if 'filename_medium' not in item: item['filename_medium'] = fname
                # -------------------------------------------------------

                # Retrieve AI analysis from cache for this specific file
                cached_data = analysis_cache.get(fname, {})
                
                # Use provided AI data if available (e.g. from Admin Dashboard)
                if req_ai_data:
                    if isinstance(req_ai_data, dict):
                        cached_data = req_ai_data
                    elif isinstance(req_ai_data, str):
                        try: cached_data = json.loads(req_ai_data)
                        except: pass
                
                # Determine final metadata: Request > Cache > Default
                final_name = req_name if req_name else cached_data.get('name', '')
                raw_desc = req_description if req_description else (cached_data.get('description') or cached_data.get('Description', ''))
                # Clean up multi-line descriptions from AI
                final_desc = str(raw_desc).split('\n')[0].strip()
                print(f"DEBUG: Publishing {fname} with clean description: {final_desc}")
                final_color = req_color if req_color else cached_data.get('color', '')

                # Fix: Prevent generic names if API returns them
                if final_name and final_name.lower().strip() in ['image', 'picture', 'photo', 'untitled']:
                    final_name = cached_data.get('category', 'Creative Asset')

                final_keywords = req_keywords if req_keywords else cached_data.get('keywords', '')
                
                # Smart Category Resolution:
                # 1. Explicit content_category from request
                # 2. AI detected category from cache
                # 3. Fallback to the routing category (e.g., 'image')
                final_category = req_content_category
                if not final_category:
                    final_category = cached_data.get('category')
                if not final_category:
                    final_category = category

                # Insert into DB (Files are already in R2)
                if category == 'image':
                    cursor.execute('INSERT INTO uploads (user_id, name, description, color_code, key_word, resolution, quality, category, link_small, link_medium, link_original, link_tiny, upload_date, ai_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', 
                                (session['user_id'], final_name, final_desc, final_color, final_keywords, item['resolution'], item['quality'], final_category, item['filename_small'], item['filename_medium'], item['filename_original'], item.get('filename_tiny'), time.time(), json.dumps(cached_data)))
                elif category == 'logo':
                    cursor.execute('INSERT INTO uploads (user_id, name, description, color_code, key_word, category, link_small, link_medium, link_original, upload_date, ai_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', 
                                (session['user_id'], final_name, final_desc, final_color, final_keywords, final_category, item['filename_small'], item['filename_medium'], item['filename_original'], time.time(), json.dumps(cached_data)))
                
                success_count += 1
                
                # Invalidate search cache
                search_index_cache['last_updated'] = 0
                index_path = os.path.join(DB_FOLDER, 'search_index_v2.json')
                if os.path.exists(index_path):
                    try: os.remove(index_path)
                    except: pass

                # Cleanup Pending DB
                try:
                    with sqlite3.connect(PENDING_DB) as p_conn:
                        p_conn.execute("DELETE FROM pending WHERE filename = ?", (fname,))
                        p_conn.commit()
                except: pass

            except Exception as e:
                errors.append(f"Error processing {fname}: {str(e)}")
            finally:
                # Cleanup: Remove the API thumbnail and cache entry
                thumb_path = os.path.join(TEMP_FOLDER, f"api_thumb_{fname}")
                if os.path.exists(thumb_path):
                    try:
                        os.remove(thumb_path)
                    except Exception:
                        pass

                # Cleanup: Remove the copy in temp_queue_storage
                try:
                    queue_folder = queue_manager.temp_storage_path
                    if os.path.exists(queue_folder):
                        for q_file in os.listdir(queue_folder):
                            if q_file.endswith(f"_{fname}"):
                                try:
                                    os.remove(os.path.join(queue_folder, q_file))
                                except Exception:
                                    pass
                except Exception:
                    pass

                if fname in analysis_cache:
                    del analysis_cache[fname]
    
    conn.commit()
    conn.close()

    if success_count > 0:
        save_analysis_cache()
        return jsonify({'success': True, 'message': f'Successfully published {success_count} files.'}), 200
    else:
        return jsonify({'success': False, 'message': 'PUBLISH_FAILED', 'errors': errors}), 500

# -------------------------------------------------------------------
# Serve File Route: Retrieves images from the uploads folder
# -------------------------------------------------------------------
@app.route('/uploads/<path:filename>')
def uploaded_file(filename):
    """Serves an uploaded file."""
    # Handle Tiny Request (Mobile Optimization)
    if request.args.get('size') == 'tiny':
        base = filename
        if base.startswith('small_'): base = base[6:]
        elif base.startswith('medium_'): base = base[7:]
        
        base_no_ext = os.path.splitext(base)[0]
        tiny_filename = f"tiny_{base_no_ext}.webp"
        
        # We attempt to serve the tiny version. If it doesn't exist (old uploads), 
        # the frontend onerror handler will fallback to the original request.
        filename = tiny_filename

    # 0. Proxy Mode (For Editor/Canvas CORS)
    if request.args.get('proxy') == '1' and s3_client and R2_BUCKET_NAME:
        try:
            file_obj = s3_client.get_object(Bucket=R2_BUCKET_NAME, Key=filename)
            return Response(
                file_obj['Body'].read(),
                mimetype=file_obj.get('ContentType', 'image/jpeg')
            )
        except Exception as e:
            print(f"Proxy error for {filename}: {e}")

    # 1. Priority: Generate Presigned URL (Authenticated Access)
    # This ensures access even if the bucket is private or R2.dev is disabled.
    if s3_client and R2_BUCKET_NAME:
        try:
            url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': R2_BUCKET_NAME, 'Key': filename},
                ExpiresIn=3600
            )
            return redirect(url)
        except Exception as e:
            print(f"Error generating presigned URL: {e}")

    # 2. Fallback: Public Domain (If s3_client fails but domain exists)
    if R2_DOMAIN:
        return redirect(f"{R2_DOMAIN}/{filename}")

    # 3. Last Resort: Local File (Only if R2 is completely missing)
    local_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    if os.path.exists(local_path):
        response = send_from_directory(app.config['UPLOAD_FOLDER'], filename)
        response.headers['Cache-Control'] = 'public, max-age=31536000, immutable'
        return response

    print(f"DEBUG: 404 Error for {filename}. R2_DOMAIN={bool(R2_DOMAIN)}, s3_client={bool(s3_client)}, Local={os.path.exists(local_path)}")
    return "File not found", 404

@app.route('/temp_preview/<path:filename>')
def serve_temp_preview(filename):
    """
    Serves a file from the temporary folder for preview purposes.
    SECURITY: This should only be used for previews of generated/uploaded content
    before it is published. The cleanup job will eventually delete these files.
    """
    # Sanitize filename to prevent directory traversal
    safe_filename = secure_filename(filename)
    if not safe_filename:
        return "Invalid filename", 400

    response = send_from_directory(TEMP_FOLDER, safe_filename)
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return response

@app.route('/download/<category>/<int:asset_id>')
def download_asset(category, asset_id):
    """
    Handles downloading the best quality version of an asset.
    Increments download count and serves the file as an attachment.
    """
    if category not in DB_MAPPING:
        return "Invalid category", 400

    try:
        conn = sqlite3.connect(DB_MAPPING[category])
        cursor = conn.cursor()
        
        # Fetch file info - link_original is the best quality
        cursor.execute("SELECT link_original, name, user_id FROM uploads WHERE id = ?", (asset_id,))
        row = cursor.fetchone()
        
        if not row:
            conn.close()
            return "Asset not found", 404
            
        filename, asset_name, owner_id = row
        
        # Increment Download Count
        cursor.execute("UPDATE uploads SET downloads = downloads + 1 WHERE id = ?", (asset_id,))
        conn.commit()
        conn.close()
        
        # Notification Logic
        current_user_id = session.get('user_id')
        if owner_id and owner_id != current_user_id:
            create_notification(owner_id, f"A user downloaded your {category}: {asset_name}", 'download')

        # Construct a safe download filename
        ext = filename.rsplit('.', 1)[1] if '.' in filename else 'jpg'
        safe_name = secure_filename(asset_name)
        if not safe_name:
            safe_name = f"download-{asset_id}"
        download_name = f"{safe_name}.{ext}"

        # 1. Priority: Generate Presigned URL with Attachment Header (Forces Download)
        if s3_client and R2_BUCKET_NAME:
            try:
                url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': R2_BUCKET_NAME, 
                        'Key': filename,
                        'ResponseContentDisposition': f'attachment; filename="{download_name}"'
                    },
                    ExpiresIn=3600
                )
                return redirect(url)
            except Exception as e:
                print(f"Error generating presigned URL: {e}")

        # 2. Fallback: Public Domain (If s3_client fails but domain exists)
        if R2_DOMAIN:
            return redirect(f"{R2_DOMAIN}/{filename}")

        # 3. Last Resort: Local File
        local_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if os.path.exists(local_path):
            return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True, download_name=download_name)

        return "File not found", 404
    except Exception as e:
        print(f"Error processing download: {e}")
        return "Internal Server Error", 500

@app.route('/api/assets/<category>')
def get_assets(category):
    """Returns a list of assets from the database for a specific category."""
    if category not in DB_MAPPING:
        return jsonify({'success': False, 'message': 'Invalid category'}), 400
    
    # --- RATE LIMITING & SECURITY PROTOCOL ---
    client_ip = request.remote_addr
    current_time = time.time()
    
    if client_ip not in rate_limit_store:
        rate_limit_store[client_ip] = {'blocked_until': 0, 'history': []}
    
    client_data = rate_limit_store[client_ip]
    
    # 1. Check if currently blocked
    if current_time < client_data['blocked_until']:
        remaining = int(client_data['blocked_until'] - current_time)
        return jsonify({'success': False, 'message': f'Security Alert: Too many requests. Blocked for {remaining}s.'}), 429
    
    # 2. Prune history (keep only last 0.5 seconds)
    # The limit is strict: > 50 assets in 0.5s triggers a block.
    window_size = 2.0
    client_data['history'] = [entry for entry in client_data['history'] if entry[0] > current_time - window_size]
    
    # 3. Get requested limit & page
    try:
        limit = int(request.args.get('limit', 5)) # Default limit 5 as requested
        sort_order = request.args.get('sort', 'newest') # Support for random sorting
        if request.args.get('offset'):
            offset = int(request.args.get('offset'))
        else:
            page = int(request.args.get('page', 1))
            offset = (page - 1) * limit
    except ValueError:
        limit = 5
        offset = 0
        
    # 4. Check Threshold
    current_load = sum(entry[1] for entry in client_data['history'])
    if current_load + limit > 600:
        client_data['blocked_until'] = current_time + 60 # Block for 1 minute
        return jsonify({'success': False, 'message': 'Security Alert: Rate limit exceeded. Blocked for 1 minute.'}), 429
        
    # 5. Log request
    client_data['history'].append((current_time, limit))
    # -----------------------------------------

    # Determine Sort Order
    order_clause = "ORDER BY upload_date DESC"
    if sort_order == 'random':
        order_clause = "ORDER BY RANDOM()"
    elif sort_order == 'popular':
        order_clause = "ORDER BY views DESC, likes DESC"
    elif sort_order == 'downloads':
        order_clause = "ORDER BY downloads DESC"

    target_user_id = request.args.get('user_id')
    # Get current language
    current_lang = g.get('lang', 'en')

    try:
        conn = sqlite3.connect(DB_MAPPING[category])
        conn.row_factory = sqlite3.Row # Allows accessing columns by name
        cursor = conn.cursor()
        
        # Use logged-in user ID or Guest ID (if available)
        user_id = session.get('user_id')
        if not user_id:
            user_id = session.get('guest_id', 0)
        
        # Attach users database to join and get user details
        conn.execute(f"ATTACH DATABASE '{USERS_DB}' AS users_db")
        
        sql = f'''
            SELECT t.id, t.user_id, 
            t.name, t.description, t.color_code, t.key_word, t.link_tiny,
            t.resolution, t.quality, t.category, t.link_small, t.link_medium, t.link_original, t.upload_date, t.likes, t.views, t.downloads, t.ai_data,
            '{category}' as category_type, users.username, users.avatar,
            EXISTS(SELECT 1 FROM users_db.user_likes WHERE user_id = ? AND asset_id = t.id AND category = ?) as is_liked,
            EXISTS(SELECT 1 FROM users_db.user_saves WHERE user_id = ? AND asset_id = t.id AND category = ?) as is_saved
            FROM uploads t
            LEFT JOIN users_db.users users ON t.user_id = users.id 
        '''
        
        params = [user_id, category, user_id, category]

        if target_user_id:
            sql += " WHERE t.user_id = ?"
            params.append(target_user_id)

        sql += f" {order_clause} LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        cursor.execute(sql, params)
        assets = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        return jsonify({'success': True, 'assets': clean_asset_list(assets)})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/similar/<category>/<int:asset_id>')
def get_similar_assets(category, asset_id):
    """Returns a list of similar assets based on category."""
    if category not in DB_MAPPING:
        return jsonify({'success': False, 'message': 'Invalid category'}), 400

    try:
        conn = sqlite3.connect(DB_MAPPING[category])
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # First, get the category of the original asset
        cursor.execute("SELECT category FROM uploads WHERE id = ?", (asset_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return jsonify({'success': False, 'message': 'Asset not found'}), 404
        
        asset_category = row['category']

        # Now, get a few random assets from the same category
        cursor.execute(f"""
            SELECT id, name, link_small, category, resolution, '{category}' as category_type
            FROM uploads
            WHERE category = ? AND id != ?
            ORDER BY RANDOM()
            LIMIT 6
        """, (asset_category, asset_id))
        
        assets = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return jsonify({'success': True, 'assets': assets})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

def get_orientation(resolution_str):
    """Calculates orientation from a resolution string (e.g., '1920x1080')."""
    if not resolution_str or 'x' not in str(resolution_str):
        return None
    try:
        width, height = map(int, resolution_str.lower().split('x'))
        if width > height: return 'Horizontal'
        elif height > width: return 'Vertical'
        else: return 'Square'
    except ValueError:
        return None

def check_orientation(resolution_str, target_orientation):
    """Parses resolution string (e.g., '1920x1080') and checks orientation."""
    orientation = get_orientation(resolution_str)
    return orientation == target_orientation

# Cache for search index and vocabulary
search_index_cache = {
    'data': [],
    'words': set(),
    'last_updated': 0
}

def get_search_index():
    """Fetches and caches full asset data for search and vocabulary."""
    now = time.time()
    index_file = os.path.join(DB_FOLDER, 'search_index_v2.json')

    # 1. Check Memory Cache
    if now - search_index_cache['last_updated'] < 86400 and search_index_cache['data']:
        return search_index_cache['data'], list(search_index_cache['words'])
    
    # 2. Check File Cache (Fast Reading from Text)
    if os.path.exists(index_file) and os.path.getsize(index_file) > 0:
        try:
            if now - os.path.getmtime(index_file) < 86400:
                with open(index_file, 'r', encoding='utf-8') as f:
                    # Support new dict format or legacy list format
                    content = json.load(f)
                    
                    if isinstance(content, list):
                        data = content
                    else:
                        data = content.get('data', [])
                    
                    # Rebuild vocab from data (including translations)
                    vocab = set()
                    for item in data:
                        if item.get('name'): vocab.update(item['name'].lower().split())
                        if item.get('keywords'): vocab.update(item['keywords'].lower().replace(',', ' ').split())
                        if item.get('category'): vocab.add(item['category'].lower())
                        if item.get('color'): vocab.add(item['color'].lower())
                    
                    search_index_cache['data'] = data
                    search_index_cache['words'] = vocab
                    search_index_cache['last_updated'] = now
                    return data, list(vocab)
        except Exception:
            pass # Fallback to DB if file is corrupt or old

    # 3. Build from Database (Slower)
    data = []
    vocab = set()
    
    for cat, db_name in DB_MAPPING.items():
        try:
            conn = sqlite3.connect(db_name)
            conn.row_factory = sqlite3.Row
            conn.execute(f"ATTACH DATABASE '{USERS_DB}' AS users_db")
            cursor = conn.cursor()
            
            # Select resolution only if it exists (Images have it, Logos might not)
            cols = "t.id, t.name, t.description, t.key_word, t.category, t.color_code, t.ai_data, u.username"
            if cat == 'image': cols += ", t.resolution"
            
            cursor.execute(f"SELECT {cols} FROM uploads t LEFT JOIN users_db.users u ON t.user_id = u.id")
            rows = cursor.fetchall()
            conn.close()
            
            for row in rows:
                # Fallback: Check ai_data for description if column is empty
                description = row['description']
                if not description and row['ai_data']:
                    try:
                        ai_data = json.loads(row['ai_data'])
                        description = ai_data.get('description', '')
                    except:
                        pass

                # Calculate Orientation for Index
                res = dict(row).get('resolution', "")
                orientation = get_orientation(res) if res else ""

                # Build rich object for the index file
                item = {
                    "id": row['id'],
                    "type": cat,
                    "name": row['name'] or "",
                    "description": description or "",
                    "keywords": row['key_word'] or "",
                    "category": row['category'] or "",
                    "color": row['color_code'] or "",
                    "resolution": res,
                    "orientation": orientation
                }
                data.append(item)
                
                # Build vocab for fuzzy matching
                if item['name']: vocab.update(item['name'].lower().split())
                if item['description']: vocab.update(item['description'].lower().split())
                if item['keywords']: vocab.update(item['keywords'].lower().replace(',', ' ').split())
                if item['category']: vocab.add(item['category'].lower())
                if item['color']: vocab.add(item['color'].lower())
                if row['username']: vocab.add(row['username'].lower())
        except Exception:
            pass
            
    # 4. Save as Rich JSON for Fast Reading next time
    try:
        with open(index_file, 'w', encoding='utf-8') as f:
            # Save data with embedded translations
            json.dump({'data': data}, f, indent=2)
    except Exception as e:
        print(f"Error saving search vocabulary: {e}")

    search_index_cache['data'] = data
    search_index_cache['words'] = vocab
    search_index_cache['last_updated'] = now
    return data, list(vocab)

# Refined word lists for intent analysis
NOISE_WORDS = {
    # Articles & Prepositions
    'a', 'an', 'the', 'and', 'or', 'of', 'in', 'on', 'at', 'to', 'with', 'by', 
    'for', 'from', 'up', 'down', 'over', 'under', 'about', 'into', 'through', 
    'during', 'before', 'after', 'above', 'below', 'between', 'out', 'off',
    # Pronouns
    'i', 'my', 'me', 'we', 'our', 'us', 'you', 'your', 'yours', 'he', 'him', 
    'his', 'she', 'her', 'hers', 'it', 'its', 'they', 'them', 'their', 'theirs',
    'this', 'that', 'these', 'those', 'who', 'whom', 'whose', 'which', 'what',
    # Verbs (Auxiliary/Common)
    'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 
    'do', 'does', 'did', 'can', 'could', 'will', 'would', 'shall', 'should',
    'may', 'might', 'must', 'get', 'got', 'getting', 'make', 'made', 'making',
    'am', 'im', 'ive', 'id', 'ill',
    # Conversational / Action
    'show', 'find', 'search', 'look', 'looking', 'want', 'need', 'give', 
    'please', 'thanks', 'hello', 'hi', 'hey', 'help', 'display', 'fetch',
    'finding', 'searching', 'wanted', 'needing',
    # Adverbs / Conjunctions / Misc
    'just', 'only', 'very', 'really', 'too', 'so', 'as', 'like', 'than', 
    'but', 'if', 'then', 'else', 'when', 'where', 'why', 'how', 'here', 'there',
    'many', 'other', 'some', 'all', 'any', 'each', 'every', 'few', 'lot', 'lots',
    'thing', 'things', 'stuff',
    # Contraction parts
    'm', 's', 't', 're', 've', 'll', 'd', 'don', 'won'
}

BROAD_TERMS = {
    # Generic Nouns
    'image', 'images', 'picture', 'pictures', 'photo', 'photos', 'pic', 'pics',
    'wallpaper', 'wallpapers', 'background', 'backgrounds', 'screensaver', 'screensavers',
    'snapshot', 'snapshots', 'shot', 'shots', 'photograph', 'photographs',
    'visual', 'visuals', 'graphic', 'graphics', 'illustration', 'illustrations',
    'art', 'artwork', 'design', 'designs', 'drawing', 'drawings',
    'img', 'imgs', 'bg', 'bgs', 'wall', 'walls', 'pfp',
    # Quality/Style Generic
    'high', 'quality', 'best', 'top', 'good', 'great', 'cool', 'nice', 'awesome',
    'beautiful', 'pretty', 'amazing', 'stunning', 'epic', 'perfect', 'super',
    'free', 'download', 'online', 'full', 'hd', 'uhd', '4k', '8k', '2k', '1080p',
    'hq', 'high-res', 'hi-res', 'resolution', 'def', 'definition'
}

COMMON_COLORS = {
    'red', 'orange', 'yellow', 'green', 'blue', 'purple', 'pink', 'brown', 'black', 'white', 
    'gray', 'grey', 'gold', 'silver', 'bronze', 'teal', 'cyan', 'magenta', 'maroon', 'navy', 
    'olive', 'beige', 'cream', 'ivory', 'vibrant', 'dark', 'light', 'neon', 'pastel', 'matte', 'glossy'
}

SIZE_MAPPING = {
    'Horizontal': {
        # Devices
        'desktop', 'pc', 'computer', 'laptop', 'monitor', 'screen',

        # Orientation / shape
        'horizontal', 'landscape', 'wide', 'widescreen', 'panoramic',
        'cinematic', 'long wide', 'flat',

        # Use-case language
        'desktop wallpaper', 'pc wallpaper', 'computer background',
        'wallpaper for pc', 'background for desktop', 'full screen',

        # Setup / environment
        'desk', 'workspace', 'gaming setup', 'battlestation', 'office',

        # Aspect & ratios
        '16:9', '21:9', '32:9', 'ultrawide', 'ultra wide', 'wide monitor'
    },

    'Vertical': {
        # Devices
        'mobile', 'phone', 'smartphone', 'iphone', 'android',

        # Orientation / shape
        'vertical', 'portrait', 'tall', 'long', 'upright',

        # Use-case language
        'phone wallpaper', 'mobile wallpaper', 'lockscreen',
        'lock screen', 'homescreen', 'home screen',
        'background for phone',

        # Social platform habits
        'story', 'stories', 'reel', 'reels', 'tiktok', 'shorts',

        # Aspect & ratios
        '9:16', '10:16', '3:4', '4:5'
    },

    'Square': {
        # Devices
        'tablet', 'ipad',

        # Orientation / shape
        'square', 'even', 'balanced',

        # Platform habits
        'instagram', 'insta', 'post', 'feed',

        # Aspect ratios
        '1:1'
    }
}

SPELLING_CORRECTIONS = {
    'img': 'image', 'imgs': 'images',
    'pic': 'picture', 'pics': 'pictures',
    'bg': 'background', 'bgs': 'backgrounds',
    'wall': 'wallpaper', 'walls': 'wallpapers',
    'wallpeper': 'wallpaper', 'walpaper': 'wallpaper',
    'backround': 'background', 'bakground': 'background',
    'awsome': 'awesome', 'beutiful': 'beautiful', 'beatiful': 'beautiful',
    'coler': 'color', 'colour': 'color',
    'meny': 'many', 'thinf': 'thing',
    'fav': 'favorite', 'favs': 'favorites'
}

def analyze_search_query(query, vocab_list=None, vocab_set=None):
    """
    Analyzes the raw search query to extract intent, filters, and core keywords.
    Distinguishes between specific searches ("dark volcano") and broad browsing ("wallpaper").
    """
    if not query:
        return {'terms': [], 'colors': [], 'quality': None, 'size': None, 'is_broad_only': False}

    q_lower = query.lower()
    
    # 1. Clean Punctuation
    for char in '.,-!?:;\'"':
        q_lower = q_lower.replace(char, ' ')

    detected_size = None
    
    # Check Size/Orientation (Phrase Matching)
    # Flatten and sort keywords by length (longest first) to handle phrases like "lock screen" before "screen"
    all_size_keywords = []
    for size_key, keywords in SIZE_MAPPING.items():
        for kw in keywords:
            all_size_keywords.append((kw, size_key))
    all_size_keywords.sort(key=lambda x: len(x[0]), reverse=True)
    
    for kw, size_key in all_size_keywords:
        # Regex to match whole words/phrases
        pattern = r'(?<!\w)' + re.escape(kw) + r'(?!\w)'
        if re.search(pattern, q_lower):
            detected_size = size_key
            q_lower = re.sub(pattern, ' ', q_lower)
            break

    raw_words = q_lower.split()
    
    # Enhanced Spelling Correction
    corrected_words = []
    for w in raw_words:
        # 1. Hardcoded Corrections
        if w in SPELLING_CORRECTIONS:
            corrected_words.append(SPELLING_CORRECTIONS[w])
            continue
            
        # 2. Dynamic Correction (if vocab provided)
        if vocab_list and vocab_set and w not in vocab_set and w not in NOISE_WORDS:
            # Try to find a close match in existing content
            matches = difflib.get_close_matches(w, vocab_list, n=1, cutoff=0.85)
            if matches:
                corrected_words.append(matches[0])
                continue
        
        corrected_words.append(w)
    raw_words = corrected_words

    # 2. Extract Quality (Token-based)
    detected_quality = None
    quality_map = {
        '4k': '4K', '8k': '4K', 'uhd': '4K',
        '2k': '2K', 'qhd': '2K', '1440p': '2K',
        'hd': 'HD', 'fhd': 'HD', '1080p': 'HD'
    }
    
    clean_words = []
    for w in raw_words:
        if w in quality_map:
            if not detected_quality: detected_quality = quality_map[w]
            continue
        clean_words.append(w)

    # 3. Extract Colors & Filter Noise
    detected_colors = []
    final_terms = []
    
    for w in clean_words:
        if w in COMMON_COLORS:
            detected_colors.append(w)
        
        if w not in NOISE_WORDS and w not in BROAD_TERMS:
            final_terms.append(w)

    return {'terms': final_terms, 'colors': detected_colors, 'quality': detected_quality, 'size': detected_size, 'is_broad_only': (len(final_terms) == 0)}

def slugify(text):
    """Converts text to a slug (e.g., 'Hello World!' -> 'hello-world')."""
    if not text: return "asset"
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s-]', '', text)
    text = re.sub(r'[\s-]+', '-', text)
    return text.strip('-')

def perform_search_logic(query, category_filter, quality_filter, size_filter, sort_order, user_id):
    results = []
    
    targets = []
    cat_lower = category_filter.lower()
    
    if cat_lower == 'all':
        targets = ['image', 'logo']
    elif 'logo' in cat_lower:
        targets = ['logo']
    else:
        targets = ['image']
        
    if category_filter == '4K Image':
        quality_filter = '4K'

    # Fetch vocab for smart analysis
    _, vocab_list = get_search_index()
    vocab_set = set(vocab_list)
    analysis = analyze_search_query(query, vocab_list, vocab_set)
    
    if not quality_filter and analysis['quality'] and category_filter != '4K Image':
        quality_filter = analysis['quality']
        
    if not size_filter and analysis['size']:
        size_filter = analysis['size']

    search_terms = analysis['terms']
    detected_colors = analysis['colors']

    for cat in targets:
        db_path = DB_MAPPING.get(cat)
        if not db_path: continue
        
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute(f"ATTACH DATABASE '{USERS_DB}' AS users_db")
            cursor = conn.cursor()
            
            # Define columns based on category schema (Logos don't have resolution/quality columns)
            if cat == 'image':
                extra_cols = "t.resolution, t.quality,"
            else:
                extra_cols = "NULL as resolution, NULL as quality,"

            sql = f"""
                SELECT t.id, t.user_id, 
                t.name, t.description, t.color_code, t.key_word, t.link_tiny,
                {extra_cols} t.category, t.link_small, t.link_medium, t.link_original, t.upload_date, t.likes, t.views, t.downloads, t.ai_data,
                '{cat}' as category_type, u.username, u.avatar,
                EXISTS(SELECT 1 FROM users_db.user_likes WHERE user_id = ? AND asset_id = t.id AND category = '{cat}') as is_liked,
                EXISTS(SELECT 1 FROM users_db.user_saves WHERE user_id = ? AND asset_id = t.id AND category = '{cat}') as is_saved
                FROM uploads t
                LEFT JOIN users_db.users u ON t.user_id = u.id
                WHERE 1=1
            """
            params = [user_id, user_id]
            
            if search_terms:
                _, vocab = get_search_index()
                for term in search_terms:
                    matches = difflib.get_close_matches(term.lower(), vocab, n=3, cutoff=0.6)
                    search_variations = set([term] + matches)
                    term_group_sql = []
                    for var in search_variations:
                        wildcard = f"%{var}%"
                        term_group_sql.append(f"(t.name LIKE ? OR t.description LIKE ? OR t.key_word LIKE ? OR t.category LIKE ? OR t.color_code LIKE ? OR u.username LIKE ?)")
                        params.extend([wildcard, wildcard, wildcard, wildcard, wildcard, wildcard])
                    if term_group_sql:
                        sql += " AND (" + " OR ".join(term_group_sql) + ")"
            
            if cat == 'image' and quality_filter:
                sql += " AND t.quality = ?"
                params.append(quality_filter)

            if category_filter not in ['All', 'Logo', '4K Image']:
                sql += " AND LOWER(t.category) = LOWER(?)"
                params.append(category_filter)
                
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            
            for row in rows:
                asset = dict(row)
                if size_filter:
                    res_str = asset.get('resolution')
                    if res_str:
                        if not check_orientation(res_str, size_filter):
                            continue
                    elif cat == 'image':
                        # Fallback: Check metadata if resolution is missing
                        meta_text = (asset.get('key_word') or '') + ' ' + (asset.get('name') or '')
                        meta_lower = meta_text.lower()
                        if size_filter == 'Vertical' and not any(x in meta_lower for x in ['vertical', 'mobile', 'phone']):
                             continue
                        elif size_filter == 'Horizontal' and not any(x in meta_lower for x in ['horizontal', 'desktop', 'pc']):
                             continue
                        elif size_filter == 'Square' and not any(x in meta_lower for x in ['square', 'instagram']):
                             continue
                results.append(asset)
            conn.close()
        except Exception as e:
            print(f"Search error in {cat}: {e}")
            
    for asset in results:
        score = 0
        name = str(asset.get('name') or '').lower()
        keywords = str(asset.get('key_word') or '').lower()
        category = str(asset.get('category') or '').lower()
        color_code = str(asset.get('color_code') or '').lower()
        description = str(asset.get('description') or '').lower()
        username = str(asset.get('username') or '').lower()
        full_text = f"{name} {description} {keywords} {category} {username}"
        
        for term in search_terms:
            if term in full_text: score += 10
            if term in name: score += 20
            if term in keywords: score += 15
            if term in description: score += 5
            if term in username: score += 25
            
        for color in detected_colors:
            if color in color_code: score += 50
            elif color in full_text: score += 10
        
        asset['relevance_score'] = score

    sort_key_map = {'newest': 'upload_date', 'popular': 'views', 'downloads': 'downloads'}
    primary_sort_key = sort_key_map.get(sort_order, 'upload_date')

    if query:
        if sort_order == 'popular':
            results.sort(key=lambda x: (x['relevance_score'], x.get('views', 0), x.get('likes', 0)), reverse=True)
        else:
            results.sort(key=lambda x: (x['relevance_score'], x.get(primary_sort_key, 0)), reverse=True)
    else:
        if sort_order == 'popular':
            results.sort(key=lambda x: (x.get('views', 0), x.get('likes', 0)), reverse=True)
        else:
            results.sort(key=lambda x: x.get(primary_sort_key, 0), reverse=True)
            
    # Random Fallback if results are scarce
    MIN_RESULTS = 12
    if len(results) < MIN_RESULTS:
        needed = MIN_RESULTS - len(results)
        
        # Determine fallback category
        fallback_cats = ['image']
        if category_filter and 'logo' in category_filter.lower():
            fallback_cats = ['logo']
        elif category_filter and category_filter.lower() == 'all':
            fallback_cats = ['image', 'logo']
            
        random_results = []
        
        for cat in fallback_cats:
            if len(random_results) >= needed: break
            db_path = DB_MAPPING.get(cat)
            if not db_path: continue
            
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                conn.execute(f"ATTACH DATABASE '{USERS_DB}' AS users_db")
                cursor = conn.cursor()
                
                sql = f"""
                    SELECT t.id, t.user_id, t.link_tiny,
                    t.name, t.description, t.color_code, t.key_word,
                    t.resolution, t.quality, t.category, t.link_small, t.link_medium, t.link_original, t.upload_date, t.likes, t.views, t.downloads, t.ai_data,
                    EXISTS(SELECT 1 FROM users_db.user_saves WHERE user_id = ? AND asset_id = t.id AND category = '{cat}') as is_saved
                    FROM uploads t
                    LEFT JOIN users_db.users u ON t.user_id = u.id
                    ORDER BY RANDOM() LIMIT ?
                """
                cursor.execute(sql, (user_id, user_id, needed * 3))
                rows = cursor.fetchall()
                conn.close()
                
                for row in rows:
                    if len(random_results) >= needed: break
                    asset = dict(row)
                    # Avoid duplicates
                    if not any(r['id'] == asset['id'] and r['category_type'] == asset['category_type'] for r in results + random_results):
                        asset['is_random'] = True
                        random_results.append(asset)
            except Exception as e:
                print(f"Random fetch error: {e}")
        
        results.extend(random_results)

    # Final cleaning pass
    return clean_asset_list(results)

@app.route('/api/search')
def search_assets():
    """Searches for assets based on query, category, and filters."""
    query = request.args.get('q', '').strip()
    category_filter = request.args.get('category', 'All')
    quality_filter = request.args.get('resolution') # '4K', 'HD', etc.
    size_filter = request.args.get('size') # 'Horizontal', 'Vertical', 'Square'
    sort_order = request.args.get('sort', 'newest')
    user_id = session.get('user_id', session.get('guest_id', 0))
    
    results = perform_search_logic(query, category_filter, quality_filter, size_filter, sort_order, user_id)
    
    return jsonify({'success': True, 'assets': results})

@app.route('/api/save_generated_image', methods=['POST'])
def save_generated_image():
    """Saves a generated image to R2 (private folder) and records it in the generated DB."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Authentication required'}), 401

    data = request.get_json()
    filename = data.get('filename')
    prompt = data.get('prompt', '')

    if not filename:
        return jsonify({'success': False, 'message': 'Filename is required'}), 400

    # Sanitize and locate file in temp folder
    filename = secure_filename(filename)
    file_path = os.path.join(TEMP_FOLDER, filename)

    if not os.path.exists(file_path):
        return jsonify({'success': False, 'message': 'File not found or expired'}), 404

    try:
        # Define private R2 path (separated from public uploads)
        user_id = session['user_id']
        # Using a 'private' prefix to ensure it doesn't mix with public gallery images
        r2_key = f"private/generated/{user_id}/{filename}"
        
        # Upload to R2
        if s3_client:
            upload_to_r2(file_path, r2_key)
        else:
            return jsonify({'success': False, 'message': 'Storage service unavailable'}), 500

        # Save to Generated DB
        with sqlite3.connect(GENERATED_DB) as conn:
            conn.execute(
                "INSERT INTO user_generations (user_id, filename, prompt, r2_key, created_at) VALUES (?, ?, ?, ?, ?)",
                (user_id, filename, prompt, r2_key, time.time())
            )
        
        return jsonify({'success': True, 'message': 'Image saved to your private library.'})

    except Exception as e:
        print(f"Error saving generated image: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/search')
def search_page():
    """Renders the home page with search results pre-calculated for SEO."""
    query = request.args.get('q', '').strip()
    category = request.args.get('category', 'All')
    resolution = request.args.get('resolution')
    size = request.args.get('size')
    sort = request.args.get('sort', 'newest')

    if not query:
        return redirect(url_for('index'))
        
    # Get common template vars (duplicated from index for safety)
    has_unread = False
    theme_color = '#0a84ff'
    theme_mode = 'dark'
    user_id = session.get('user_id')

    if user_id:
        try:
            conn = sqlite3.connect(NOTIF_DB)
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM notifications WHERE user_id = ? AND is_read = 0 LIMIT 1", (user_id,))
            if cursor.fetchone(): has_unread = True
            conn.close()

            if 'theme_color' in session: theme_color = session['theme_color']
            conn = sqlite3.connect(USERS_DB)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT theme_color, theme_mode FROM user_preferences WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            if row:
                if row['theme_color']: theme_color = row['theme_color']
                if row['theme_mode']: theme_mode = row['theme_mode']
            conn.close()
        except Exception: pass

    # Perform Search to get count
    results = perform_search_logic(query, category, resolution, size, sort, user_id or 0)
    
    return render_template('home.html', 
                           has_unread=has_unread, 
                           theme_color=theme_color, 
                           theme_mode=theme_mode, 
                           r2_domain=R2_DOMAIN,
                           search_query=query,
                           result_count=len(results),
                           current_page=1)

@app.route('/generator')
@app.route('/ai')
def generator_page():
    """Direct link to open the AI Generator."""
    has_unread = False
    theme_color = '#0a84ff'
    theme_mode = 'light'
    user_id = session.get('user_id')

    if user_id:
        try:
            conn = sqlite3.connect(NOTIF_DB)
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM notifications WHERE user_id = ? AND is_read = 0 LIMIT 1", (user_id,))
            if cursor.fetchone(): has_unread = True
            conn.close()

            if 'theme_color' in session: theme_color = session['theme_color']
            # We skip full prefs fetch for speed, session usually has it
        except Exception: pass

    return render_template('home.html', 
                           has_unread=has_unread, 
                           theme_color=theme_color, 
                           theme_mode=theme_mode, 
                           r2_domain=R2_DOMAIN,
                           open_generator=True,
                           current_page=1)

@app.route('/view/<category>/<int:asset_id>')
@app.route('/view/<category>/<int:asset_id>/<slug>')
def view_asset(category, asset_id, slug=None):
    """
    SEO Landing Page for a specific asset.
    Renders home.html but pre-loads metadata for bots and auto-opens the modal for humans.
    """
    if category not in DB_MAPPING:
        return redirect(url_for('index'))

    asset = None
    try:
        conn = sqlite3.connect(DB_MAPPING[category])
        conn.row_factory = sqlite3.Row
        conn.execute(f"ATTACH DATABASE '{USERS_DB}' AS users_db")
        cursor = conn.cursor()
        
        # Fetch full asset details including user info
        sql = f"""
            SELECT t.*, '{category}' as category_type, u.username, u.avatar
            FROM uploads t
            LEFT JOIN users_db.users u ON t.user_id = u.id
            WHERE t.id = ?
        """
        cursor.execute(sql, (asset_id,))
        row = cursor.fetchone()
        if row:
            asset = dict(row)
        conn.close()
    except Exception as e:
        print(f"Error fetching asset for view: {e}")

    if not asset:
        return redirect(url_for('index'))

    # SEO: Enforce canonical URL with slug
    # If the URL doesn't have the correct slug, redirect to the one that does.
    correct_slug = slugify(asset.get('name', 'asset'))
    if slug != correct_slug:
        return redirect(url_for('view_asset', category=category, asset_id=asset_id, slug=correct_slug), code=301)

    # Render home but pass the asset for SEO tags and JS auto-open
    return render_template('home.html', 
                           view_asset=asset,
                           r2_domain=R2_DOMAIN,
                           theme_color=session.get('theme_color', '#0a84ff'),
                           theme_mode=session.get('theme_mode', 'light'),
                           current_page=1)

def generate_sitemap_xml():
    """Helper to generate sitemap XML string."""
    host = request.url_root.rstrip('/')
    xml = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">')
    
    # 1. Add Home
    xml.append(f'<url><loc>{host}/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>')
    
    # 2. Add Assets
    for cat, db_path in DB_MAPPING.items():
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            # Get latest 1000 items per category, fetching medium and original links for SEO.
            cursor.execute("SELECT id, name, link_medium, link_original, upload_date FROM uploads ORDER BY upload_date DESC LIMIT 1000")
            rows = cursor.fetchall()
            conn.close()
            
            for row in rows:
                # row[0]=id, row[1]=name, row[2]=medium, row[3]=original, row[4]=date

                # The Page URL (Landing Page)
                safe_slug = slugify(row[1])
                loc = f"{host}/view/{cat}/{row[0]}/{safe_slug}"
                # The Image URL (Direct File)
                # Prefer Original quality for best SEO, fallback to Medium
                img_file = row[3] if row[3] else row[2]
                if not img_file:
                    continue # Skip if no image file is found
                img_loc = f"{R2_DOMAIN}/{img_file}" if R2_DOMAIN else f"{host}/uploads/{img_file}"
                
                # Last Modified Date
                lastmod = time.strftime('%Y-%m-%d', time.localtime(row[4])) if row[4] else None
                
                xml.append('<url>')
                xml.append(f'<loc>{loc}</loc>')
                if lastmod:
                    xml.append(f'<lastmod>{lastmod}</lastmod>')
                xml.append('<image:image>')
                xml.append(f'<image:loc>{img_loc}</image:loc>')
                if row[1]:
                    # Escape special characters for XML
                    safe_title = str(row[1]).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                    xml.append(f'<image:title>{safe_title}</image:title>')
                xml.append('</image:image>')
                xml.append('</url>')
        except Exception: pass

    xml.append('</urlset>')
    return '\n'.join(xml)

@app.route('/sitemap.xml')
def sitemap():
    """Serves sitemap.xml, generating it if static file is missing."""
    static_sitemap = os.path.join(app.root_path, 'sitemap.xml')
    if os.path.exists(static_sitemap):
        return send_file(static_sitemap, mimetype='text/xml')
    
    return Response(generate_sitemap_xml(), mimetype='text/xml')

@app.route('/api/admin/generate_sitemap', methods=['POST'])
def admin_generate_sitemap():
    try:
        xml_content = generate_sitemap_xml()
        with open(os.path.join(app.root_path, 'sitemap.xml'), 'w', encoding='utf-8') as f:
            f.write(xml_content)
        return jsonify({'success': True, 'message': 'Sitemap generated successfully.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/home-sections')
def home_sections():
    """Returns HTML content for different sections on the home page."""
    sections_data = []
    
    # List of template files for the sections you want to load dynamically
    section_templates = ['4kimgsection.html']
    
    # Fetch 4K assets
    assets_4k = []
    try:
        conn = sqlite3.connect(DB_MAPPING['image'])
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT link_medium FROM uploads WHERE quality = "4K" ORDER BY upload_date DESC LIMIT 6')
        assets_4k = [dict(row) for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        print(f"Error fetching 4K assets: {e}")

    try:
        # Prepare data to pass to the template
        template_data = {
            'assets_4k': assets_4k
        }

        # Render the template to a string, passing the data
        html_content = render_template('4kimgsection.html', **template_data)
        sections_data.append({'html': html_content})

        return jsonify({'success': True, 'sections': sections_data})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error loading sections: {str(e)}'}), 500

@app.route('/profile-template')
def profile_template():
    """Returns the profile HTML template."""
    username = session.get('username', 'Guest')
    # Ensure avatar path is clean before passing to template
    avatar = session.get('avatar')
    if avatar and isinstance(avatar, str) and avatar.startswith('/static/avatars/'):
        avatar = avatar.replace('/static/avatars/', '')

    user_id = session.get('user_id')
    role = session.get('role')

    # Dynamic Security: Refresh role from DB to ensure UI is up to date
    if user_id:
        try:
            with sqlite3.connect(USERS_DB) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT role, email FROM users WHERE id = ?", (user_id,))
                row = cursor.fetchone()
                if row: 
                    session['role'] = row[0]
                    session['email'] = row[1]
                    role = row[0]
        except Exception: pass

    # Get the first letter for the avatar, default to 'G' if empty
    avatar_letter = username[0].upper() if username else 'G'
    
    total_likes = 0
    total_views = 0
    total_downloads = 0
    latest_notification = "Welcome! Check here for updates."
    latest_notification_type = "bell"
    
    if user_id:
        for category, db_name in DB_MAPPING.items():
            try:
                conn = sqlite3.connect(db_name)
                cursor = conn.cursor()
                cursor.execute("SELECT SUM(likes), SUM(views), SUM(downloads) FROM uploads WHERE user_id = ?", (user_id,))
                row = cursor.fetchone()
                if row:
                    total_likes += row[0] if row[0] else 0
                    total_views += row[1] if row[1] else 0
                    total_downloads += row[2] if row[2] else 0
                conn.close()
            except Exception as e:
                print(f"Error calculating stats for {category}: {e}")
        
        # Fetch latest notification
        try:
            conn = sqlite3.connect(NOTIF_DB)
            cursor = conn.cursor()
            cursor.execute("SELECT message, type FROM notifications WHERE user_id = ? ORDER BY created_at DESC LIMIT 1", (user_id,))
            row = cursor.fetchone()
            if row:
                latest_notification = row[0]
                db_type = row[1]
                if db_type == 'like': latest_notification_type = 'heart'
                elif db_type == 'download': latest_notification_type = 'download'
                elif db_type == 'view': latest_notification_type = 'eye'
                else: latest_notification_type = 'bell'
            conn.close()
        except Exception as e:
            print(f"Error fetching notification: {e}")

    return render_template('profile.html', username=username, avatar_letter=avatar_letter, avatar=avatar, total_likes=total_likes, total_views=total_views, total_downloads=total_downloads, latest_notification=latest_notification, latest_notification_type=latest_notification_type, r2_domain=R2_DOMAIN, role=role)

@app.route('/user-library/<int:user_id>')
def user_library(user_id):
    """Renders the user library page for a specific user."""
    try:
        conn = sqlite3.connect(USERS_DB)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT id, username, avatar FROM users WHERE id = ?", (user_id,))
        user = cursor.fetchone()
        conn.close()

        if not user:
            return "User not found", 404

        # Fetch assets for the user (you might want to paginate this)
        assets = []
        for category, db_name in DB_MAPPING.items():
            conn = sqlite3.connect(db_name)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT id, name, link_small FROM uploads WHERE user_id = ?", (user_id,))
            assets.extend([dict(row) for row in cursor.fetchall()])
            conn.close()

        return render_template('user_library.html', user=dict(user), assets=assets)
    except Exception as e:
        print(f"Error loading user library: {e}")
        return "Internal Server Error", 500

@app.route('/api/users/<int:user_id>/assets')
def get_public_user_assets(user_id):
    """API to fetch paginated assets for a specific user."""
    limit = int(request.args.get('limit', 20))
    offset = int(request.args.get('offset', 0))
    
    all_assets = []
    
    # Fetch from all databases
    for category, db_name in DB_MAPPING.items():
        try:
            conn = sqlite3.connect(db_name)
            conn.row_factory = sqlite3.Row
            # Attach users DB to check likes/saves if logged in
            conn.execute(f"ATTACH DATABASE '{USERS_DB}' AS users_db")
            cursor = conn.cursor()
            
            current_user = session.get('user_id', 0)
            
            cursor.execute(f'''
                SELECT uploads.*, '{category}' as category_type,
                (SELECT 1 FROM users_db.user_likes WHERE user_id = ? AND asset_id = uploads.id AND category = ?) as is_liked,
                (SELECT 1 FROM users_db.user_saves WHERE user_id = ? AND asset_id = uploads.id AND category = ?) as is_saved
                FROM uploads 
                WHERE user_id = ?
            ''', (current_user, category, current_user, category, user_id))
            
            all_assets.extend([dict(row) for row in cursor.fetchall()])
            conn.close()
        except Exception as e:
            print(f"Error fetching {category} for user {user_id}: {e}")

    # Sort by date (newest first) and paginate in Python
    all_assets.sort(key=lambda x: x['upload_date'], reverse=True)
    paginated_assets = all_assets[offset : offset + limit]
    
    return jsonify({'success': True, 'assets': paginated_assets, 'has_more': len(all_assets) > offset + limit})

# ===================================================================================
#                                 USER AUTHENTICATION
# ===================================================================================

@app.route('/login/google')
def google_login():
    """Redirects to Google's OAuth 2.0 server to start the login process."""
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        return "Google OAuth is not configured on the server.", 500

    flow = Flow.from_client_config(
        client_config=client_secrets,
        scopes=GOOGLE_SCOPES,
        redirect_uri=url_for('google_callback', _external=True)
    )

    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true'
    )
    session['state'] = state
    return redirect(authorization_url)

@app.route('/login/google/callback')
def google_callback():
    """Handles the callback from Google after user authentication."""
    if 'state' not in session:
        print("Google Login Error: State missing from session.")
        return redirect(url_for('index'))
    state = session.pop('state', None)

    try:
        flow = Flow.from_client_config(
            client_config=client_secrets,
            scopes=GOOGLE_SCOPES,
            state=state,
            redirect_uri=url_for('google_callback', _external=True)
        )

        # Fix for HTTP/HTTPS mismatch behind proxies (Render/Cloudflare)
        authorization_response = request.url
        if request.headers.get('X-Forwarded-Proto') == 'https' and authorization_response.startswith('http:'):
            authorization_response = authorization_response.replace('http:', 'https:', 1)

        flow.fetch_token(authorization_response=authorization_response)
        credentials = flow.credentials
    except Exception as e:
        print(f"Google OAuth Error: {e}")
        return redirect(url_for('index'))
    
    request_session = google_requests.Request()
    id_info = id_token.verify_oauth2_token(
        id_token=credentials.id_token,
        request=request_session,
        audience=GOOGLE_CLIENT_ID
    )

    email = id_info.get('email')
    name = id_info.get('name')
    avatar_url = id_info.get('picture')

    conn = sqlite3.connect(USERS_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM users WHERE email = ?", (email,))
    user = cursor.fetchone()
    
    if user:
        # User exists, log them in
        session.permanent = True
        session['user_id'] = user['id']
        session['username'] = user['username']
        session['role'] = user['role']
        session['avatar'] = user['avatar']
    else:
        # New user, create account
        cursor.execute("SELECT 1 FROM users WHERE username = ?", (name,))
        username = f"{name}{random.randint(100,999)}" if cursor.fetchone() else name
        
        avatar_filename = random.choice(AVATAR_LIBRARY) # Default avatar
        if avatar_url:
            try:
                img_response = requests.get(avatar_url)
                if img_response.status_code == 200:
                    avatar_filename = f"user_google_{int(time.time())}.jpg"
                    with open(os.path.join(AVATAR_STORAGE_FOLDER, avatar_filename), 'wb') as f:
                        f.write(img_response.content)
            except Exception: pass

        hashed_pw = generate_password_hash(str(uuid.uuid4()))
        cursor.execute("INSERT INTO users (username, email, password_hash, avatar, role, created_at) VALUES (?, ?, ?, ?, 'user', ?)", (username, email, hashed_pw, avatar_filename, time.time()))
        new_user_id = cursor.lastrowid
        conn.commit()
        create_notification(new_user_id, "Welcome to the community! We are glad to have you here.", "system")
        
        session.permanent = True
        session['user_id'] = new_user_id
        session['username'] = username
        session['role'] = 'user'
        session['avatar'] = avatar_filename

    conn.close()
    return redirect(url_for('index'))

@app.route('/register', methods=['POST'])
def register():
    """Handles user registration."""
    data = request.get_json() if request.is_json else request.form
    username = data.get('username')
    password = data.get('password')
    email = data.get('email')
    
    # Assign a random avatar from the library
    avatar = random.choice(AVATAR_LIBRARY)
    
    result = user_manager.create_user(username, password, email, avatar=avatar)
    if result['success']:
        # Add Welcome Notification
        try:
            conn = sqlite3.connect(USERS_DB)
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
            row = cursor.fetchone()
            conn.close()
            
            if row:
                user_id = row[0]
                conn_notif = sqlite3.connect(NOTIF_DB)
                conn_notif.execute("INSERT INTO notifications (user_id, message, type, created_at) VALUES (?, ?, ?, ?)", 
                                     (user_id, "Welcome to the community! We are glad to have you here.", "system", time.time()))
                conn_notif.commit()
                conn_notif.close()
        except Exception as e:
            print(f"Error creating welcome notification: {e}")
            
        # Auto-login the user
        session.permanent = True
        session['user_id'] = result['user_id']
        session['username'] = username
        session['role'] = 'user'
        session['avatar'] = avatar
        
        # Return user info for frontend storage
        result['username'] = username
        result['avatar'] = avatar
        result['role'] = 'user'

        return jsonify(result), 200
    return jsonify(result), 400

@app.route('/login', methods=['POST'])
def login():
    """Handles user login."""
    data = request.get_json() if request.is_json else request.form
    username = data.get('username')
    password = data.get('password')
    
    result = user_manager.authenticate_user(username, password)
    if result['success']:
        session.permanent = True
        session['user_id'] = result['user_id']
        session['username'] = result.get('username', username)
        session['role'] = result['role']
        session['avatar'] = result.get('avatar')
        return jsonify(result), 200
    result['message'] = 'INVALID_CREDENTIALS'
    return jsonify(result), 401

@app.route('/logout')
def logout():
    """Logs out the current user."""
    session.clear()
    return redirect(url_for('index'))

@app.route('/api/avatars')
def get_avatars():
    """Returns the list of available cartoon avatars."""
    return jsonify({'success': True, 'avatars': AVATAR_LIBRARY})

@app.route('/update-avatar', methods=['POST'])
def update_avatar():
    """Updates the logged-in user's avatar."""
    if 'username' not in session:
        return jsonify({'success': False, 'message': 'Not logged in'}), 401
    
    data = request.get_json()
    new_avatar = data.get('avatar')
    
    if new_avatar in AVATAR_LIBRARY:
        user_manager.update_avatar(session['username'], new_avatar)
        session['avatar'] = new_avatar
        return jsonify({'success': True})
    return jsonify({'success': False, 'message': 'Invalid avatar'}), 400

@app.route('/update-profile', methods=['POST'])
def update_profile():
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'AUTH_REQUIRED'}), 401
    
    data = request.get_json() if request.is_json else request.form
    new_username = data.get('username')
    new_password = data.get('password')
    current_password = data.get('current_password')
    new_bio = data.get('bio')
    website = data.get('website')
    instagram = data.get('instagram')
    twitter = data.get('twitter')
    contact_email = data.get('contact_email')
    
    if not new_username:
        return jsonify({'success': False, 'message': 'Username cannot be empty'}), 400
        
    try:
        conn = sqlite3.connect(USERS_DB)
        cursor = conn.cursor()
        
        # Check if username is taken by another user
        cursor.execute("SELECT id FROM users WHERE username = ? AND id != ?", (new_username, session['user_id']))
        if cursor.fetchone():
            conn.close()
            return jsonify({'success': False, 'message': 'Username already taken'}), 400
            
        # Update Username
        cursor.execute("UPDATE users SET username = ? WHERE id = ?", (new_username, session['user_id']))

        # Handle Avatar Upload
        if 'avatar' in request.files:
            file = request.files['avatar']
            if file and file.filename != '' and allowed_file(file.filename):
                # Generate unique filename
                ext = file.filename.rsplit('.', 1)[1].lower()
                filename = secure_filename(f"user_{session['user_id']}_{int(time.time())}.{ext}")
                
                # Save to the persistent avatar storage folder
                file.save(os.path.join(AVATAR_STORAGE_FOLDER, filename))
                
                db_avatar = filename
                cursor.execute("UPDATE users SET avatar = ? WHERE id = ?", (db_avatar, session['user_id']))
                session['avatar'] = db_avatar

        # Update Bio
        if new_bio is not None:
            try:
                cursor.execute("UPDATE users SET bio = ? WHERE id = ?", (new_bio, session['user_id']))
            except sqlite3.OperationalError:
                cursor.execute("ALTER TABLE users ADD COLUMN bio TEXT")
                cursor.execute("UPDATE users SET bio = ? WHERE id = ?", (new_bio, session['user_id']))
        
        # Update Socials (Ensure columns exist first)
        try:
            cursor.execute("UPDATE users SET website=?, instagram=?, twitter=?, contact_email=? WHERE id=?", 
                          (website, instagram, twitter, contact_email, session['user_id']))
        except sqlite3.OperationalError:
            cursor.execute("ALTER TABLE users ADD COLUMN website TEXT")
            cursor.execute("ALTER TABLE users ADD COLUMN instagram TEXT")
            cursor.execute("ALTER TABLE users ADD COLUMN twitter TEXT")
            cursor.execute("ALTER TABLE users ADD COLUMN contact_email TEXT")
            cursor.execute("UPDATE users SET website=?, instagram=?, twitter=?, contact_email=? WHERE id=?", 
                          (website, instagram, twitter, contact_email, session['user_id']))
        
        # Update Password if provided
        if new_password:
            hashed_pw = generate_password_hash(new_password)
            cursor.execute("UPDATE users SET password_hash = ? WHERE id = ?", (hashed_pw, session['user_id']))
            
        conn.commit()
        conn.close()
        
        session['username'] = new_username
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/static/avatars/<path:filename>')
def serve_avatar(filename):
    """
    Unified avatar serving route.
    Checks persistent storage first (for user uploads), then falls back to standard static folder (for cartoons).
    """
    # 1. Check Persistent Storage (User Uploads)
    if os.path.exists(os.path.join(AVATAR_STORAGE_FOLDER, filename)):
        return send_from_directory(AVATAR_STORAGE_FOLDER, filename)
    
    # 2. Fallback to Static Folder (Default Cartoons)
    return send_from_directory(os.path.join(app.root_path, 'static', 'avatars'), filename)

@app.route('/api/user/<int:user_id>/details')
def get_public_user_details(user_id):
    """Fetches public details for a user profile."""
    try:
        conn = sqlite3.connect(USERS_DB)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Try selecting with new columns, fallback if they don't exist yet
        try:
            cursor.execute("SELECT id, username, avatar, bio, website, instagram, twitter, contact_email FROM users WHERE id = ?", (user_id,))
        except sqlite3.OperationalError:
            cursor.execute("SELECT id, username, avatar, bio FROM users WHERE id = ?", (user_id,))
            
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return jsonify({'success': True, 'user': dict(row)})
        return jsonify({'success': False, 'message': 'User not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/notifications')
def get_notifications():
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'AUTH_REQUIRED'}), 401
    
    try:
        conn = sqlite3.connect(NOTIF_DB)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Mark notifications as read
        conn.execute("UPDATE notifications SET is_read = 1 WHERE user_id = ?", (session['user_id'],))
        conn.commit()
        
        cursor.execute("SELECT * FROM notifications WHERE user_id = ? ORDER BY created_at DESC", (session['user_id'],))
        rows = cursor.fetchall()
        notifications = [dict(row) for row in rows]
        conn.close()
        return jsonify({'success': True, 'notifications': notifications})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/track/view', methods=['POST'])
def track_view():
    # Allow guests to track views
    # if 'user_id' not in session: return jsonify({'success': False}), 401
        
    data = request.get_json()
    asset_id = data.get('asset_id')
    category = data.get('category')
    
    if not asset_id or category not in DB_MAPPING:
        return jsonify({'success': False}), 400
        
    try:
        # Increment View
        conn = sqlite3.connect(DB_MAPPING[category])
        cursor = conn.cursor()
        cursor.execute("UPDATE uploads SET views = views + 1 WHERE id = ?", (asset_id,))
        
        # Get Owner
        cursor.execute("SELECT user_id FROM uploads WHERE id = ?", (asset_id,))
        row = cursor.fetchone()
        conn.commit()
        conn.close()
        
        if row:
            owner_id = row[0]
            # Check Milestones
            total_views = 0
            for cat, db in DB_MAPPING.items():
                c = sqlite3.connect(db)
                cur = c.cursor()
                cur.execute("SELECT SUM(views) FROM uploads WHERE user_id = ?", (owner_id,))
                r = cur.fetchone()
                if r and r[0]: total_views += r[0]
                c.close()
            
            # Check specific milestones
            milestones = [100, 1000, 10000]
            if total_views in milestones:
                 create_notification(owner_id, f"Congratulations! You have reached {total_views} total views!", 'view')
                 
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/track/download', methods=['POST'])
def track_download():
    # Allow guests to track downloads
    # if 'user_id' not in session: return jsonify({'success': False}), 401
        
    data = request.get_json()
    asset_id = data.get('asset_id')
    category = data.get('category')
    
    if not asset_id or category not in DB_MAPPING:
        return jsonify({'success': False}), 400
        
    try:
        conn = sqlite3.connect(DB_MAPPING[category])
        cursor = conn.cursor()
        cursor.execute("UPDATE uploads SET downloads = downloads + 1 WHERE id = ?", (asset_id,))
        
        cursor.execute("SELECT user_id, name FROM uploads WHERE id = ?", (asset_id,))
        row = cursor.fetchone()
        conn.commit()
        conn.close()
        
        if row:
            owner_id, asset_name = row
            current_user_id = session.get('user_id')
            if owner_id != current_user_id:
                create_notification(owner_id, f"A user downloaded your {category}: {asset_name}", 'download')
                
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/like', methods=['POST'])
def toggle_like():
    # Allow guests to like
    user_id = session.get('user_id')
    is_guest = False
    
    if not user_id:
        # Generate or retrieve Guest ID
        if 'guest_id' not in session:
            # Use negative numbers for guests to avoid conflict with real user IDs
            session['guest_id'] = -random.randint(100000, 999999999)
        user_id = session['guest_id']
        is_guest = True
    
    data = request.get_json()
    asset_id = data.get('asset_id')
    category = data.get('category')
    
    if not asset_id or category not in DB_MAPPING:
        return jsonify({'success': False, 'message': 'Invalid data'}), 400

    conn_users = sqlite3.connect(USERS_DB)
    cursor_users = conn_users.cursor()
    
    # Check if liked
    cursor_users.execute('SELECT 1 FROM user_likes WHERE user_id=? AND asset_id=? AND category=?', (user_id, asset_id, category))
    exists = cursor_users.fetchone()
    
    liked = False
    if exists:
        cursor_users.execute('DELETE FROM user_likes WHERE user_id=? AND asset_id=? AND category=?', (user_id, asset_id, category))
        liked = False
    else:
        cursor_users.execute('INSERT INTO user_likes (user_id, asset_id, category) VALUES (?, ?, ?)', (user_id, asset_id, category))
        liked = True
    conn_users.commit()
    conn_users.close()
    
    # Update Asset Counter
    try:
        conn_assets = sqlite3.connect(DB_MAPPING[category])
        cursor_assets = conn_assets.cursor()
        if liked:
            cursor_assets.execute('UPDATE uploads SET likes = likes + 1 WHERE id=?', (asset_id,))
        else:
            cursor_assets.execute('UPDATE uploads SET likes = MAX(0, likes - 1) WHERE id=?', (asset_id,))
        conn_assets.commit()
        conn_assets.close()
    except Exception as e:
        print(f"Error updating asset likes: {e}")
        
    # Send Notification if liked
    if liked:
        try:
            conn_asset = sqlite3.connect(DB_MAPPING[category])
            cursor_asset = conn_asset.cursor()
            cursor_asset.execute("SELECT user_id, name FROM uploads WHERE id = ?", (asset_id,))
            row = cursor_asset.fetchone()
            conn_asset.close()
            if row:
                owner_id, asset_name = row
                if owner_id != user_id:
                    liker_name = session.get('username') if not is_guest else "A user"
                    create_notification(owner_id, f"{liker_name} liked your {category}: {asset_name}", 'like')
        except Exception as e:
            print(f"Error sending like notification: {e}")

    return jsonify({'success': True, 'liked': liked})

@app.route('/api/save', methods=['POST'])
def toggle_save():
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'AUTH_REQUIRED'}), 401
    
    data = request.get_json()
    asset_id = data.get('asset_id')
    category = data.get('category')
    user_id = session['user_id']
    
    if category == 'generated':
        try:
            conn = sqlite3.connect(GENERATED_DB)
            cursor = conn.cursor()
            # Check if exists
            cursor.execute("SELECT 1 FROM user_generations WHERE id = ? AND user_id = ?", (asset_id, user_id))
            if cursor.fetchone():
                cursor.execute("DELETE FROM user_generations WHERE id = ? AND user_id = ?", (asset_id, user_id))
                saved = False
            else:
                saved = False 
            conn.commit()
            conn.close()
            return jsonify({'success': True, 'saved': saved})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500

    conn = sqlite3.connect(USERS_DB)
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM user_saves WHERE user_id=? AND asset_id=? AND category=?', (user_id, asset_id, category))
    if cursor.fetchone():
        cursor.execute('DELETE FROM user_saves WHERE user_id=? AND asset_id=? AND category=?', (user_id, asset_id, category))
        saved = False
    else:
        cursor_users = cursor.execute('INSERT INTO user_saves (user_id, asset_id, category) VALUES (?, ?, ?)', (user_id, asset_id, category))
        saved = True
    conn.commit()
    conn.close()
    return jsonify({'success': True, 'saved': saved})

@app.route('/api/user/saved')
def get_user_saved_assets():
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'AUTH_REQUIRED'}), 401
    
    user_id = session['user_id']
    limit = int(request.args.get('limit', 20))
    offset = int(request.args.get('offset', 0))
    
    saved_assets = []
    for category, db_name in DB_MAPPING.items():
        try:
            conn = sqlite3.connect(db_name)
            conn.row_factory = sqlite3.Row
            conn.execute(f"ATTACH DATABASE '{USERS_DB}' AS users_db")
            cursor = conn.cursor()
            
            cursor.execute(f'''
                SELECT uploads.id, uploads.name, uploads.description, uploads.link_small, uploads.link_tiny, uploads.upload_date,
                uploads.likes, uploads.views, uploads.downloads,
                '{category}' as category_type, users.username, users.avatar,
                1 as is_saved,
                (SELECT 1 FROM users_db.user_likes WHERE user_id = ? AND asset_id = uploads.id AND category = ?) as is_liked
                FROM uploads
                JOIN users_db.user_saves s ON uploads.id = s.asset_id AND s.category = ? AND s.user_id = ?
                LEFT JOIN users_db.users users ON uploads.user_id = users.id
                ORDER BY upload_date DESC
            ''', (user_id, category, category, user_id))
            
            rows = cursor.fetchall()
            for row in rows:
                saved_assets.append(dict(row))
            conn.close()
        except Exception as e:
            print(f"Error fetching saved {category}: {e}")
            
    # Fetch Generated Images (Private Library)
    try:
        conn = sqlite3.connect(GENERATED_DB)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT id, filename, prompt, r2_key, created_at FROM user_generations WHERE user_id = ? ORDER BY created_at DESC", (user_id,))
        rows = cursor.fetchall()
        
        # Get user details for display consistency
        username = session.get('username', 'User')
        avatar = session.get('avatar', '')
        
        for row in rows:
            # Map to asset structure
            asset = {
                'id': row['id'], 
                'name': 'AI Generated',
                'description': row['prompt'],
                'link_small': row['r2_key'], # Use R2 key as the image link
                'link_tiny': None,
                'upload_date': row['created_at'],
                'likes': 0,
                'views': 0,
                'downloads': 0,
                'category_type': 'generated',
                'username': username,
                'avatar': avatar,
                'is_saved': 1,
                'is_liked': 0
            }
            saved_assets.append(asset)
        conn.close()
    except Exception as e:
        print(f"Error fetching generated images: {e}")

    saved_assets.sort(key=lambda x: x['upload_date'], reverse=True)
    saved_assets = clean_asset_list(saved_assets)
    paginated_assets = saved_assets[offset : offset + limit]
    
    return jsonify({'success': True, 'assets': paginated_assets, 'has_more': len(saved_assets) > offset + limit})

@app.route('/api/user/uploads')
def get_user_uploads():
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'AUTH_REQUIRED'}), 401
    
    user_id = session['user_id']
    limit = int(request.args.get('limit', 20))
    offset = int(request.args.get('offset', 0))
    
    user_uploads = []
    for category, db_name in DB_MAPPING.items():
        try:
            conn = sqlite3.connect(db_name)
            conn.row_factory = sqlite3.Row
            conn.execute(f"ATTACH DATABASE '{USERS_DB}' AS users_db")
            cursor = conn.cursor()
            
            cursor.execute(f'''
                SELECT uploads.id, uploads.name, uploads.description, uploads.link_small, uploads.link_tiny, uploads.upload_date,
                uploads.likes, uploads.views, uploads.downloads,
                '{category}' as category_type,
                (SELECT 1 FROM users_db.user_likes WHERE user_id = ? AND asset_id = uploads.id AND category = ?) as is_liked,
                (SELECT 1 FROM users_db.user_saves WHERE user_id = ? AND asset_id = uploads.id AND category = ?) as is_saved
                FROM uploads
                WHERE user_id = ?
                ORDER BY upload_date DESC
            ''', (user_id, category, user_id, category, user_id))
            
            rows = cursor.fetchall()
            for row in rows:
                user_uploads.append(dict(row))
            conn.close()
        except Exception as e:
            print(f"Error fetching user uploads for {category}: {e}")
            
    user_uploads.sort(key=lambda x: x['upload_date'], reverse=True)
    paginated_assets = user_uploads[offset : offset + limit]
    
    return jsonify({'success': True, 'assets': paginated_assets, 'has_more': len(user_uploads) > offset + limit})

@app.route('/api/delete_asset', methods=['POST'])
def delete_asset():
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'AUTH_REQUIRED'}), 401

    data = request.get_json()
    asset_id = data.get('asset_id')
    category = data.get('category')

    if not asset_id or category not in DB_MAPPING:
        return jsonify({'success': False, 'message': 'Invalid request'}), 400

    try:
        db_path = DB_MAPPING[category]
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT user_id, link_small, link_medium, link_original, link_tiny FROM uploads WHERE id = ?", (asset_id,))
        asset = cursor.fetchone()

        if not asset:
            conn.close()
            return jsonify({'success': False, 'message': 'Asset not found'}), 404

        # Convert sqlite3.Row to dict to allow .get() method and avoid AttributeError
        asset = dict(asset)

        if asset['user_id'] != session['user_id']:
            conn.close()
            return jsonify({'success': False, 'message': 'Permission denied'}), 403

        files_to_delete = [asset.get('link_small'), asset.get('link_medium'), asset.get('link_original'), asset.get('link_tiny')]
        
        if s3_client and R2_BUCKET_NAME:
            try:
                keys = [{'Key': f} for f in files_to_delete if f]
                if keys:
                    s3_client.delete_objects(Bucket=R2_BUCKET_NAME, Delete={'Objects': keys})
            except Exception as e:
                print(f"Error deleting from R2: {e}")

        for f in files_to_delete:
            if f:
                local_path = os.path.join(app.config['UPLOAD_FOLDER'], f)
                if os.path.exists(local_path):
                    try:
                        os.remove(local_path)
                    except Exception:
                        pass

        cursor.execute("DELETE FROM uploads WHERE id = ?", (asset_id,))
        conn.commit()
        conn.close()

        # Cleanup interactions (likes/saves) and reports to ensure full SQL deletion
        try:
            conn_users = sqlite3.connect(USERS_DB)
            conn_users.execute("DELETE FROM user_likes WHERE asset_id = ? AND category = ?", (asset_id, category))
            conn_users.execute("DELETE FROM user_saves WHERE asset_id = ? AND category = ?", (asset_id, category))
            conn_users.commit()
            conn_users.close()
            
            conn_reports = sqlite3.connect(REPORT_DB)
            conn_reports.execute("DELETE FROM reports WHERE asset_id = ? AND category = ?", (asset_id, category))
            conn_reports.commit()
            conn_reports.close()
        except Exception as e:
            print(f"Error cleaning up related records: {e}")

        return jsonify({'success': True})

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/user/preferences', methods=['GET', 'POST'])
def user_preferences():
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'AUTH_REQUIRED'}), 401
    
    user_id = session['user_id']
    
    if request.method == 'POST':
        data = request.get_json()
        theme_color = data.get('theme_color')
        theme_mode = data.get('theme_mode')
            
        try:
            conn = sqlite3.connect(USERS_DB)
            cursor = conn.cursor()
            
            # Check if exists
            cursor.execute("SELECT 1 FROM user_preferences WHERE user_id = ?", (user_id,))
            exists = cursor.fetchone()
            
            if exists:
                if theme_color:
                    cursor.execute("UPDATE user_preferences SET theme_color = ? WHERE user_id = ?", (theme_color, user_id))
                if theme_mode:
                    cursor.execute("UPDATE user_preferences SET theme_mode = ? WHERE user_id = ?", (theme_mode, user_id))
            else:
                cursor.execute("INSERT INTO user_preferences (user_id, theme_color, theme_mode) VALUES (?, ?, ?)", 
                               (user_id, theme_color, theme_mode))
            
            conn.commit()
            conn.close()
            
            if theme_color: session['theme_color'] = theme_color
            
            return jsonify({'success': True})
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500
            
    return jsonify({'success': False, 'message': 'Method not allowed'}), 405

@app.route('/api/report', methods=['POST'])
def submit_report():
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'AUTH_REQUIRED'}), 401
    
    data = request.get_json()
    asset_id = data.get('asset_id')
    category = data.get('category')
    reasons = data.get('reasons', []) # List of strings, default to empty list
    message = data.get('message', '') # Default to empty string
    
    if not asset_id or not category:
        return jsonify({'success': False, 'message': 'Invalid asset'}), 400
        
    try:
        conn = sqlite3.connect(REPORT_DB)
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO reports (user_id, asset_id, category, reasons, message, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (session['user_id'], asset_id, category, json.dumps(reasons), message, time.time())
        )
        conn.commit()
        conn.close()
        print(f"Report saved successfully: User {session['user_id']} reported Asset {asset_id}")
        return jsonify({'success': True, 'message': 'Report submitted successfully'})
    except Exception as e:
        print(f"Error saving report: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/feedback', methods=['POST'])
def submit_feedback():
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'AUTH_REQUIRED'}), 401
    
    data = request.get_json()
    message = data.get('message', '')
    
    if not message.strip():
        return jsonify({'success': False, 'message': 'Message cannot be empty'}), 400
        
    try:
        conn = sqlite3.connect(REPORT_DB)
        cursor = conn.cursor()
        # Insert feedback as a report with category 'feedback'
        cursor.execute(
            "INSERT INTO reports (user_id, asset_id, category, reasons, message, created_at) VALUES (?, NULL, 'feedback', ?, ?, ?)",
            (session['user_id'], json.dumps(['General Feedback']), message, time.time())
        )
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': 'Feedback submitted successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/admin/reports')
def admin_reports():
    try:
        conn = sqlite3.connect(REPORT_DB)
        conn.row_factory = sqlite3.Row
        # Attach users db to get usernames
        conn.execute(f"ATTACH DATABASE '{USERS_DB}' AS users_db")
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT r.*, u.username, u.email 
            FROM reports r 
            LEFT JOIN users_db.users u ON r.user_id = u.id 
            ORDER BY r.created_at DESC
        ''')
        
        reports = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        # Parse reasons JSON for frontend
        for r in reports:
            try:
                r['reasons'] = json.loads(r['reasons'])
            except:
                r['reasons'] = []
                
        return jsonify({'success': True, 'reports': reports})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/admin/delete_report', methods=['POST'])
def admin_delete_report():
    data = request.get_json()
    report_id = data.get('id')
    
    try:
        conn = sqlite3.connect(REPORT_DB)
        conn.execute("DELETE FROM reports WHERE id = ?", (report_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

# ===================================================================================
#                                 ADMIN DASHBOARD
# ===================================================================================

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check if user is logged in
        if 'user_id' not in session:
            return redirect(url_for('index'))
        
        # Simplified Admin Check: Trust Session Data First (Matches Frontend Visibility)
        if session.get('role') == 'admin' or session.get('email') in ADMIN_EMAILS:
            return f(*args, **kwargs)

        # Fallback: Verify role from DB if session is stale
        try:
            with sqlite3.connect(USERS_DB) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT role, email FROM users WHERE id = ?", (session['user_id'],))
                row = cursor.fetchone()
                if row:
                    session['role'] = row[0]
                    session['email'] = row[1]
                    if row[0] == 'admin' or row[1] in ADMIN_EMAILS:
                        return f(*args, **kwargs)
        except Exception as e:
            print(f"Admin auth error: {e}")

        return render_template('home.html', error="Access Denied: Admins Only")
    return decorated_function

@app.route('/admin')
@admin_required
def admin_dashboard():
    return render_template('admin_dashboard.html', r2_domain=R2_DOMAIN)

@app.route('/admin/setup/<key>')
def promote_admin(key):
    """Secured route to promote the current user to admin."""
    # Security Key - You must use this in the URL to get admin access
    if key != "secure_setup_8829":
        return "Access Denied: Invalid Key", 403

    if 'user_id' not in session:
        return "Please log in first."
    try:
        with sqlite3.connect(USERS_DB) as conn:
            conn.execute("UPDATE users SET role = 'admin' WHERE id = ?", (session['user_id'],))
        session['role'] = 'admin'
        return "Success! You are now an admin. <a href='/admin'>Go to Dashboard</a>"
    except Exception as e:
        return f"Error: {e}"

@app.route('/api/admin/stats')
@admin_required
def admin_stats():
    stats = {'users': 0, 'images': 0, 'logos': 0, 'downloads': 0, 'generations': 0}
    try:
        # Users
        with sqlite3.connect(USERS_DB) as conn:
            stats['users'] = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        
        # Images
        with sqlite3.connect(DB_MAPPING['image']) as conn:
            row = conn.execute("SELECT COUNT(*), SUM(downloads) FROM uploads").fetchone()
            stats['images'] = row[0]
            stats['downloads'] += (row[1] or 0)
            
        # Logos
        with sqlite3.connect(DB_MAPPING['logo']) as conn:
            row = conn.execute("SELECT COUNT(*), SUM(downloads) FROM uploads").fetchone()
            stats['logos'] = row[0]
            stats['downloads'] += (row[1] or 0)
        
        # Generations
        with sqlite3.connect(GENERATED_DB) as conn:
            stats['generations'] = conn.execute("SELECT COUNT(*) FROM generation_requests").fetchone()[0]
            
    except Exception as e:
        print(f"Stats Error: {e}")
        
    return jsonify({'success': True, 'stats': stats})

@app.route('/api/admin/users')
@admin_required
def admin_users():
    try:
        conn = sqlite3.connect(USERS_DB)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT id, username, email, role, avatar, created_at FROM users ORDER BY id DESC LIMIT 100")
        users = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify({'success': True, 'users': users})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/admin/delete_user', methods=['POST'])
@admin_required
def admin_delete_user():
    data = request.get_json()
    user_id = data.get('user_id')
    if not user_id: return jsonify({'success': False}), 400
    
    # Prevent self-deletion
    if user_id == session['user_id']:
        return jsonify({'success': False, 'message': 'Cannot delete yourself'}), 400
        
    try:
        conn = sqlite3.connect(USERS_DB)
        conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/admin/scan_r2')
@admin_required
def admin_scan_r2():
    """Scans R2 bucket for files not present in the SQL database."""
    if not s3_client or not R2_BUCKET_NAME:
        return jsonify({'success': False, 'message': 'R2 not configured'}), 500

    try:
        # 1. Get all files from SQL
        registered_files = set()
        for db_name in DB_MAPPING.values():
            with sqlite3.connect(db_name) as conn:
                cursor = conn.cursor()
                try:
                    # Try fetching all 4 versions to be robust against schema changes
                    cursor.execute("SELECT link_original, link_medium, link_small, link_tiny FROM uploads")
                except sqlite3.OperationalError:
                    # Fallback for older DBs without link_tiny column
                    cursor.execute("SELECT link_original, link_medium, link_small FROM uploads")

                for row in cursor.fetchall():
                    for link in row:
                        if link: registered_files.add(link)

        # 1.5 Get generated files from Generated DB
        try:
            with sqlite3.connect(GENERATED_DB) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT r2_key FROM user_generations")
                for row in cursor.fetchall():
                    if row[0]: registered_files.add(row[0])
        except Exception as e:
            print(f"Error scanning generated DB for R2 scan: {e}")

        # 2. List files in R2 (Pagination for full scan)
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=R2_BUCKET_NAME)
        
        orphaned_groups = {}
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    
                    # Filter out non-image files to avoid listing backups, logs, etc.
                    extension = key.rsplit('.', 1)[-1].lower() if '.' in key else ''
                    if extension not in ALLOWED_EXTENSIONS:
                        continue

                    if key not in registered_files:
                        # Smart grouping: Determine base name by stripping prefixes/suffixes
                        key_no_ext = key.rsplit('.', 1)[0]
                        base_name = key_no_ext
                        
                        if key_no_ext.startswith('tiny_'):
                            base_name = key_no_ext[5:]
                        elif key_no_ext.startswith('small_'):
                            base_name = key_no_ext[6:]
                        elif key_no_ext.startswith('medium_'):
                            base_name = key_no_ext[7:]
                        elif key_no_ext.endswith('_original'):
                            base_name = key_no_ext[:-9]
                        
                        if base_name not in orphaned_groups:
                            orphaned_groups[base_name] = {
                                'files': [],
                                'total_size': 0,
                                'last_modified': obj['LastModified'].isoformat(),
                                'display_key': key # Default to first found
                            }
                        
                        group = orphaned_groups[base_name]
                        group['files'].append(key)
                        group['total_size'] += obj['Size']
                        
                        # Prefer 'small_' for display if available
                        if key.startswith('small_'):
                            group['display_key'] = key

        # Convert groups to list
        orphans_list = []
        for base, data in orphaned_groups.items():
            orphans_list.append({
                'display_key': data['display_key'],
                'files': data['files'], # List of all files to delete
                'size': data['total_size'],
                'last_modified': data['last_modified']
            })

        return jsonify({'success': True, 'orphans': orphans_list})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/admin/delete_orphan', methods=['POST'])
@admin_required
def admin_delete_orphan():
    """Deletes a list of orphaned files from R2."""
    data = request.get_json()
    files = data.get('files', [])
    
    if not files:
        return jsonify({'success': False, 'message': 'No files specified'}), 400
        
    try:
        if s3_client and R2_BUCKET_NAME:
            # Delete in batches of 1000 (S3 Limit)
            chunk_size = 1000
            for i in range(0, len(files), chunk_size):
                batch = files[i:i + chunk_size]
                delete_objects = {'Objects': [{'Key': k} for k in batch]}
                s3_client.delete_objects(Bucket=R2_BUCKET_NAME, Delete=delete_objects)
            
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/admin/get_pending_uploads')
@admin_required
def get_pending_uploads():
    """Fetches pending uploads that haven't been published yet."""
    try:
        with sqlite3.connect(PENDING_DB) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM pending ORDER BY created_at DESC").fetchall()
            files = []
            for row in rows:
                item = dict(row)
                try: item['r2_data'] = json.loads(item['r2_data'])
                except: pass
                files.append(item)
        return jsonify({'success': True, 'files': files})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/admin/delete_pending_upload', methods=['POST'])
@admin_required
def delete_pending_upload():
    data = request.get_json()
    filename = data.get('filename')
    try:
        with sqlite3.connect(PENDING_DB) as conn:
            conn.execute("DELETE FROM pending WHERE filename = ?", (filename,))
            conn.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

# ===================================================================================
#                                 GOOGLE INDEXING API
# ===================================================================================

INDEXING_HISTORY_FILE = os.path.join(DB_FOLDER, 'indexing_history.json')

def load_indexing_history():
    if os.path.exists(INDEXING_HISTORY_FILE):
        try:
            with open(INDEXING_HISTORY_FILE, 'r') as f:
                return json.load(f)
        except: return {}
    return {}

def save_indexing_history(history):
    try:
        with open(INDEXING_HISTORY_FILE, 'w') as f:
            json.dump(history, f)
    except: pass

@app.route('/admin/indexing')
@admin_required
def admin_indexing_page():
    """Renders the Google Indexing UI."""
    return render_template('admin_indexing.html')

@app.route('/api/admin/indexing/status')
@admin_required
def admin_indexing_status():
    """Calculates stats for the UI."""
    history = load_indexing_history()
    now = time.time()
    DAILY_LIMIT = 200
    
    # Calculate Quota (Last 24h)
    submitted_last_24h = [ts for ts in history.values() if ts > now - 86400]
    count_last_24h = len(submitted_last_24h)
    quota_left = max(0, DAILY_LIMIT - count_last_24h)
    
    # 1. Get All URLs
    urls = []
    host = request.url_root.rstrip('/')
    
    for cat, db_path in DB_MAPPING.items():
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, name FROM uploads")
                rows = cursor.fetchall()
                for row in rows:
                    slug = slugify(row[1])
                    url = f"{host}/view/{cat}/{row[0]}/{slug}"
                    urls.append(url)
        except: pass
        
    total = len(urls)
    indexed = len([u for u in urls if u in history])
    pending = total - indexed
    
    return jsonify({
        'success': True,
        'total': total,
        'indexed': indexed,
        'pending': pending,
        'daily_limit': DAILY_LIMIT,
        'quota_left': quota_left,
        'submitted_24h': count_last_24h
    })

@app.route('/api/admin/indexing/submit', methods=['POST'])
@admin_required
def admin_indexing_submit():
    """Submits a batch of URLs to Google."""
    data = request.get_json() or {}
    manual_url = data.get('url')
    
    history = load_indexing_history()
    host = request.url_root.rstrip('/')
    now = time.time()
    DAILY_LIMIT = 200
    
    # Check Quota
    submitted_last_24h = [ts for ts in history.values() if ts > now - 86400]
    quota_left = max(0, DAILY_LIMIT - len(submitted_last_24h))
    
    if quota_left <= 0:
        return jsonify({'success': False, 'message': f'Daily quota of {DAILY_LIMIT} reached. Please wait 24h.'}), 429

    batch = []

    if manual_url:
        # Manual Mode
        # Fix: Ensure URL has scheme (https://) if user forgot it
        if not manual_url.startswith(('http://', 'https://')):
            manual_url = f'https://{manual_url}'
        batch = [manual_url]
    else:
        # Bulk Mode
        # Re-fetch URLs to be safe
        urls = []
        for cat, db_path in DB_MAPPING.items():
            try:
                with sqlite3.connect(db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT id, name FROM uploads")
                    for row in cursor.fetchall():
                        urls.append(f"{host}/view/{cat}/{row[0]}/{slugify(row[1])}")
            except: pass

        # Filter pending
        pending = [u for u in urls if u not in history]
        
        if not pending:
            return jsonify({'success': True, 'message': 'All URLs are already indexed!', 'count': 0})

        # Batch (Limit to Quota)
        batch_size = min(len(pending), quota_left, 200)
        batch = pending[:batch_size]
    
    creds_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    
    if not creds_file or not os.path.exists(creds_file):
        return jsonify({'success': False, 'message': 'Service Account Credentials not found on server.'}), 500

    # Extract email for better error messages
    client_email = "Unknown Service Account"
    try:
        with open(creds_file, 'r') as f:
            client_email = json.load(f).get('client_email', client_email)
    except: pass

    try:
        credentials = service_account.Credentials.from_service_account_file(
            creds_file, scopes=["https://www.googleapis.com/auth/indexing"]
        )
        service = build("indexing", "v3", credentials=credentials)
        
        for url in batch:
            content = {"url": url, "type": "URL_UPDATED"}
            service.urlNotifications().publish(body=content).execute()
            history[url] = time.time()
            
        save_indexing_history(history)
        return jsonify({'success': True, 'message': f'Successfully submitted {len(batch)} URLs.', 'count': len(batch)})
        
    except HttpError as e:
        # Parse Google's JSON error response
        try:
            error_content = json.loads(e.content.decode('utf-8'))
            error_msg = error_content.get('error', {}).get('message', str(e))
        except:
            error_msg = str(e)

        print(f"Google Indexing API Error: {error_msg}")

        if e.resp.status == 403:
            return jsonify({
                'success': False, 
                'message': f"ACCESS DENIED (403).\n\nGoogle says: {error_msg}\n\n(Please follow the instructions in the message above)"
            }), 403
            
        return jsonify({'success': False, 'message': f"Google API Error: {error_msg}"}), e.resp.status

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/admin/indexing/check_status', methods=['POST'])
@admin_required
def admin_indexing_check_status():
    """Checks the notification status of a URL using Google Indexing API."""
    data = request.get_json() or {}
    url = data.get('url')
    
    if not url:
        return jsonify({'success': False, 'message': 'URL is required'}), 400

    # Fix scheme if missing
    if not url.startswith(('http://', 'https://')):
        url = f'https://{url}'

    creds_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    
    if not creds_file or not os.path.exists(creds_file):
        return jsonify({'success': False, 'message': 'Service Account Credentials not found on server.'}), 500

    try:
        credentials = service_account.Credentials.from_service_account_file(
            creds_file, scopes=["https://www.googleapis.com/auth/indexing"]
        )
        service = build("indexing", "v3", credentials=credentials)
        
        # Call getMetadata
        response = service.urlNotifications().getMetadata(url=url).execute()
        
        return jsonify({'success': True, 'data': response})
        
    except HttpError as e:
        # Parse Google's JSON error response
        try:
            error_content = json.loads(e.content.decode('utf-8'))
            error_msg = error_content.get('error', {}).get('message', str(e))
        except:
            error_msg = str(e)

        return jsonify({'success': False, 'message': f"Google API Error: {error_msg}"}), e.resp.status

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

# ===================================================================================
#                                 ADMIN BULK TOOLS
# ===================================================================================
@app.route('/api/admin/scan_unanalyzed')
@admin_required
def admin_scan_unanalyzed():
    """Scans all databases for entries with no AI metadata."""
    unanalyzed = []
    for cat, db_name in DB_MAPPING.items():
        try:
            with sqlite3.connect(db_name) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                # Find entries with no real AI data. '{}' is an empty JSON object.
                cursor.execute("SELECT id, link_small FROM uploads WHERE ai_data IS NULL OR ai_data = '{}' OR ai_data = ''")
                rows = cursor.fetchall()
                for row in rows:
                    asset = dict(row)
                    asset['category_type'] = cat
                    unanalyzed.append(asset)
        except Exception as e:
            return jsonify({'success': False, 'message': f'Error scanning {db_name}: {str(e)}'}), 500
    return jsonify({'success': True, 'assets': unanalyzed})

@app.route('/api/admin/bulk_update_metadata', methods=['POST'])
@admin_required
def admin_bulk_update_metadata():
    """Receives a list of assets with AI data and updates them in the DB."""
    data = request.get_json()
    if not isinstance(data, list):
        return jsonify({'success': False, 'message': 'Invalid data format, expected a list.'}), 400

    updated_count = 0
    errors = []

    # Group updates by category to minimize DB connections
    updates_by_cat = {}
    for item in data:
        cat = item.get('category_type')
        if cat not in updates_by_cat:
            updates_by_cat[cat] = []
        updates_by_cat[cat].append(item)

    for cat, items in updates_by_cat.items():
        db_path = DB_MAPPING.get(cat)
        if not db_path:
            errors.append(f"Invalid category: {cat}")
            continue
        
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                for item in items:
                    try:
                        # The category from AI is the content category
                        content_category = item.get('category', cat)

                        cursor.execute("""
                            UPDATE uploads 
                            SET name=?, description=?, key_word=?, color_code=?, category=?, ai_data=? 
                            WHERE id=?
                        """, (item.get('name'), item.get('description'), item.get('keywords'), item.get('color'), content_category, json.dumps(item), item.get('id')))
                        if cursor.rowcount > 0:
                            updated_count += 1
                    except Exception as e_item:
                        errors.append(f"Failed to update ID {item.get('id')}: {str(e_item)}")
        except Exception as e_db:
            errors.append(f"DB error for category {cat}: {str(e_db)}")

    # Invalidate search cache after updates
    if updated_count > 0:
        search_index_cache['last_updated'] = 0
        index_path = os.path.join(DB_FOLDER, 'search_index_v2.json')
        if os.path.exists(index_path):
            try: os.remove(index_path)
            except: pass

    return jsonify({'success': True, 'updated': updated_count, 'errors': errors})

@app.route('/api/admin/bulk_import', methods=['POST'])
def admin_bulk_import():
    """
    Receives a JSON list of assets (metadata + R2 links) and inserts them into the DB.
    Secured by ADMIN_TOKEN in .env.
    """
    # 1. Security Check
    token = request.headers.get('X-Admin-Token')
    env_token = os.environ.get('ADMIN_TOKEN')
    
    if not env_token or token != env_token:
        return jsonify({'success': False, 'message': 'Unauthorized: Invalid Admin Token'}), 403

    data = request.get_json()
    if not data or not isinstance(data, list):
        return jsonify({'success': False, 'message': 'Invalid JSON format. Expected a list.'}), 400

    results = []
    success_count = 0
    errors = []

    # 2. Process each item
    for item in data:
        try:
            category = item.get('category', 'image').lower()
            # Map 'logos' or others to valid DB keys
            if 'logo' in category: category = 'logo'
            else: category = 'image'

            db_path = DB_MAPPING.get(category)
            if not db_path: continue

            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                
                # Default to Admin User ID (1) if not provided
                user_id = item.get('user_id', 1)
                
                # Handle "No Data" / Pending State
                name = item.get('name') or f"Untitled {item.get('temp_id', '')}"
                description = item.get('description') or "No description available."
                keywords = item.get('keywords') or ""
                color = item.get('color') or ""
                
                # Flag as no_data if name is missing
                ai_data_val = json.dumps(item) if item.get('name') else json.dumps({"status": "no_data", "temp_id": item.get('temp_id')})

                if category == 'image':
                    cursor.execute('INSERT INTO uploads (user_id, name, description, color_code, key_word, resolution, quality, category, link_small, link_medium, link_original, link_tiny, upload_date, ai_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', 
                                (user_id, name, description, color, keywords, item.get('resolution'), item.get('quality'), category, item.get('link_small'), item.get('link_medium'), item.get('link_original'), item.get('link_tiny'), time.time(), ai_data_val))
                elif category == 'logo':
                    cursor.execute('INSERT INTO uploads (user_id, name, description, color_code, key_word, category, link_small, link_medium, link_original, upload_date, ai_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', 
                                (user_id, name, description, color, keywords, category, item.get('link_small'), item.get('link_medium'), item.get('link_original'), time.time(), ai_data_val))
                
                new_id = cursor.lastrowid
                results.append({'temp_id': item.get('temp_id'), 'db_id': new_id, 'category': category})
                success_count += 1
        except Exception as e:
            errors.append(f"Error importing {item.get('name', 'unknown')}: {str(e)}")

    return jsonify({'success': True, 'imported': success_count, 'results': results, 'errors': errors})

@app.route('/api/admin/bulk_import_session', methods=['POST'])
@admin_required
def admin_bulk_import_session():
    """
    Same as bulk_import but uses Session Auth instead of Token.
    Used by the Web UI Dashboard.
    """
    data = request.get_json()
    if not data or not isinstance(data, list):
        return jsonify({'success': False, 'message': 'Invalid JSON'}), 400

    success_count = 0
    errors = []

    for item in data:
        try:
            # Determine category
            category = item.get('category', 'image').lower()
            if 'logo' in category: category = 'logo'
            else: category = 'image'
            
            db_path = DB_MAPPING.get(category)
            if not db_path: continue

            # If the item comes from the Web UI tool, 'url' might be the filename
            # We need to map it to link_original/medium/small
            filename = item.get('url') or item.get('filename')
            
            # Basic insertion logic (Simplified for Web UI flow)
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                # We assume the file is already in uploads/ via the /upload endpoint
                # and we are just registering the metadata now.
                cursor.execute(f"UPDATE uploads SET name=?, description=?, key_word=?, color_code=?, category=?, ai_data=? WHERE link_original LIKE ?",
                               (item.get('name'), item.get('description'), item.get('keywords'), item.get('color'), category, json.dumps(item), f"%{filename}%"))
                if cursor.rowcount > 0:
                    success_count += 1
        except Exception as e:
            errors.append(str(e))
            
    return jsonify({'success': True, 'imported': success_count, 'errors': errors})

@app.route('/api/admin/bulk_update', methods=['POST'])
def admin_bulk_update():
    """Updates existing assets with AI data using their DB ID."""
    token = request.headers.get('X-Admin-Token')
    env_token = os.environ.get('ADMIN_TOKEN')
    
    if not env_token or token != env_token:
        return jsonify({'success': False, 'message': 'Unauthorized'}), 403

    data = request.get_json()
    updated_count = 0
    
    for item in data:
        asset_id = item.get('id')
        category = item.get('category', 'image').lower()
        if 'logo' in category: category = 'logo'
        else: category = 'image'
        
        db_path = DB_MAPPING.get(category)
        if db_path and asset_id:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE uploads SET name=?, description=?, key_word=?, color_code=?, category=?, ai_data=? WHERE id=?",
                               (item.get('name'), item.get('description'), item.get('keywords'), item.get('color'), item.get('category'), json.dumps(item), asset_id))
                updated_count += 1

    return jsonify({'success': True, 'updated': updated_count})

if __name__ == '__main__':
    # Only enable debug if explicitly set in environment (Default: False for safety)
    debug_mode = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    app.run(debug=debug_mode, host='0.0.0.0', use_reloader=False)