import os
import time
import uuid
import sqlite3
import logging
import random
import json
import difflib
import re
from flask import Flask, request, redirect, url_for, render_template, send_from_directory, jsonify, session, g, send_file, Response
from functools import wraps
from werkzeug.utils import secure_filename
from werkzeug.datastructures import FileStorage
from werkzeug.security import generate_password_hash
from PIL import Image
from user_manager import UserManager
from visual import VisualRecognizer
from queue_manager import UploadQueueManager
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.config import Config

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

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

# Configuration
UPLOAD_FOLDER = os.path.join(DB_FOLDER, 'uploads')
TEMP_FOLDER = 'temp_uploads'
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'svg', 'webp'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max upload size
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'your_secure_secret_key_here')  # Required for session management

# Hardcoded Admin Emails - Add more here if needed
ADMIN_EMAILS = ['qrbxb70@gmail.com', 'qrbxb71@gmail.com']

USERS_DB = os.path.join(DB_FOLDER, 'users.db')
NOTIF_DB = os.path.join(DB_FOLDER, 'notifications.db')
REPORT_DB = os.path.join(DB_FOLDER, 'report.db')
PENDING_DB = os.path.join(DB_FOLDER, 'pending_uploads.db')
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

# API Key Pool for Rotation (Add your 5+ keys here)
API_KEYS = []

# 1. Try loading specific rotated keys from env (GOOGLE_API_KEY_1, GOOGLE_API_KEY_2, etc.)
for i in range(1, 11):
    key = os.environ.get(f'GOOGLE_API_KEY_{i}')
    if key:
        API_KEYS.append(key)

# 2. Fallback to single main key
if not API_KEYS:
    main_key = os.environ.get('GOOGLE_API_KEY')
    if main_key:
        API_KEYS.append(main_key)

visual_recognizer = VisualRecognizer(api_keys=API_KEYS)

# Initialize Queue Manager
# Rate limit: 2.0s is safe because we have 6 rotated API keys
queue_manager = UploadQueueManager(visual_recognizer, rate_limit_seconds=2.0)
queue_manager.start_worker()

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

@app.before_request
def before_request():
    """Detects user language preference before every request."""
    # Fix: Clean avatar path in session if it has legacy prefix
    if 'avatar' in session and session['avatar'] and session['avatar'].startswith('/static/avatars/'):
        session['avatar'] = session['avatar'].replace('/static/avatars/', '')

    if 'lang' in session:
        g.lang = session['lang']
    else:
        # Auto-detect from browser headers
        g.lang = request.accept_languages.best_match(SUPPORTED_LANGUAGES) or DEFAULT_LANGUAGE

@app.context_processor
def inject_i18n():
    """Injects the 't' function into all HTML templates."""
    def t(key, **kwargs):
        # 1. Get dictionary for current language
        lang_data = TRANSLATIONS.get(g.get('lang', DEFAULT_LANGUAGE), {})
        # 2. Lookup key (supports nested keys like 'nav.home' if you flatten dicts, but simple for now)
        # For nested keys, you might need a helper, but let's assume flat or simple access for start
        text = lang_data.get(key, key)
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
                    ("ai_data", "TEXT")
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
    """Deletes files in temp folders older than 24 hours and syncs cache."""
    try:
        now = time.time()
        max_age = 86400  # 24 Hours (1 Day)

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
            
    except Exception:
        pass

def allowed_file(filename):
    """Checks if the file's extension is allowed."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    """Renders the home page."""
    if 'username' in session:
        print(f"DEBUG: User {session['username']} is logged in.")
    
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
            
    content = render_template('home.html', has_unread=has_unread, theme_color=theme_color, theme_mode=theme_mode, r2_domain=R2_DOMAIN)
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
        return jsonify({'success': False, 'message': 'Authentication required'}), 401

    data = request.get_json()
    filename = data.get('filename')

    if not filename:
        return jsonify({'success': False, 'message': 'Filename is required'}), 400

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
        return jsonify({'success': False, 'message': 'File not found in temp storage'}), 404

    # Check for cached analysis to save tokens
    if filename in analysis_cache:
        data = analysis_cache[filename]
        print(f"DEBUG: Cache hit for {filename} - Serving saved analysis (0 Tokens Used)")
        return jsonify({'success': True, 'data': data})

    # Add to Queue instead of processing immediately
    task_id = queue_manager.add_to_queue(file_path, session['user_id'])
    
    # Check for high traffic (more than 2 items pending)
    response = {'success': True, 'task_id': task_id, 'status': 'queued'}
    try:
        if queue_manager.queue.qsize() > 2:
            response['high_traffic'] = True
            response['message'] = "High server traffic detected. Processing will take 20-60 seconds."
    except NotImplementedError:
        pass
        
    return jsonify(response)

@app.route('/api/check_task/<task_id>')
def check_task(task_id):
    """Checks the status of a background analysis task."""
    task = queue_manager.get_task_status(task_id)
    if not task:
        return jsonify({'success': False, 'message': 'Task not found'}), 404
    
    if task['status'] == 'completed':
        return jsonify({'success': True, 'status': 'completed', 'data': task['result']})
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
                rgb_im.save(os.path.join(dest_folder, res['link_small']), 'WEBP', quality=80)
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

    try:
        with Image.open(src_path) as img:
            paths['resolution'] = f"{img.size[0]}x{img.size[1]}"
            long_side = max(img.size)
            if long_side >= 3840: paths['quality'] = '4K'
            elif long_side >= 2560: paths['quality'] = '2K'
            elif long_side >= 1920: paths['quality'] = 'FHD'
            elif long_side >= 1280: paths['quality'] = 'HD'
            
            rgb_im = img.convert('RGB')
            
            # Medium
            medium_name = f"medium_{base_name_no_ext}.webp"
            medium_path = os.path.join(directory, medium_name)
            rgb_im.thumbnail((2048, 2048))
            rgb_im.save(medium_path, 'WEBP', quality=90)
            paths['medium'] = medium_path
            paths['filename_medium'] = medium_name
            
            # Small
            small_name = f"small_{base_name_no_ext}.webp"
            small_path = os.path.join(directory, small_name)
            rgb_im.thumbnail((1024, 1024))
            rgb_im.save(small_path, 'WEBP', quality=80)
            paths['small'] = small_path
            paths['filename_small'] = small_name
            
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
        return jsonify({'success': False, 'message': 'Authentication required'}), 401

    # Optimization: Only run cleanup 10% of the time to reduce I/O overhead
    if random.random() < 0.1:
        cleanup_temp_files()

    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'No file part in the request'}), 400
    
    file: FileStorage = request.files['file']

    if file.filename == '':
        return jsonify({'success': False, 'message': 'No selected file'}), 400

    if file and allowed_file(file.filename):
        extension = file.filename.rsplit('.', 1)[1].lower()
        # Create a clean, timestamped filename for R2
        safe_name = secure_filename(file.filename.rsplit('.', 1)[0])
        filename = f"{safe_name}-{int(time.time())}.{extension}"
        file_path = os.path.join(TEMP_FOLDER, filename)

        try:
            # Save to temporary folder first
            file.save(file_path)

            # AI Validation: Ensure file is valid and recognized by Gemini
            # We skip SVG as it is a vector format not typically handled by Vision models in this context
            if extension != 'svg':
                # Create a compressed version for the API to reduce token usage and latency
                api_thumb_path = os.path.join(TEMP_FOLDER, f"api_thumb_{filename}")
                
                try:
                    with Image.open(file_path) as img:
                        if img.mode in ('RGBA', 'P'):
                            img = img.convert('RGB')
                        img.thumbnail((1024, 1024))
                        img.save(api_thumb_path, 'JPEG', quality=60)
                except Exception as e:
                    print(f"Error creating API thumbnail: {e}")

            # Note: We no longer analyze immediately here. 
            # We just confirm the upload. Analysis happens in /api/analyze via queue.
            
            # --- NEW LOGIC: DIRECT R2 UPLOAD ---
            # Process images (Resize)
            prep_res = prepare_images_for_r2(file_path, filename)
            if not prep_res:
                return jsonify({'success': False, 'message': 'Failed to process image'}), 500

            # Upload all versions to R2
            if s3_client:
                upload_to_r2(prep_res['original'], prep_res['filename_original'])
                if prep_res['medium'] and prep_res['medium'] != prep_res['original']:
                    upload_to_r2(prep_res['medium'], prep_res['filename_medium'])
                if prep_res['small'] and prep_res['small'] != prep_res['original']:
                    upload_to_r2(prep_res['small'], prep_res['filename_small'])
            
            # Cleanup local temp files immediately
            if os.path.exists(file_path): os.remove(file_path)
            if prep_res['medium'] and os.path.exists(prep_res['medium']): os.remove(prep_res['medium'])
            if prep_res['small'] and os.path.exists(prep_res['small']): os.remove(prep_res['small'])

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
            return jsonify({'success': False, 'message': f'Error saving file: {str(e)}'}), 500
    
    return jsonify({'success': False, 'message': 'File type not allowed'}), 400

# -------------------------------------------------------------------
# Publish Route: Handles confirmation, compression, and final storage
# -------------------------------------------------------------------
@app.route('/publish', methods=['POST'])
def publish_file():
    """Moves a file from temp to confirmed uploads, processes based on type, and saves to specific DB."""
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Authentication required'}), 401

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
        return jsonify({'success': False, 'message': 'Invalid category.'}), 400

    # Reload cache to ensure we have the latest analysis data from the worker
    load_analysis_cache()

    # =================================================================================
    # HANDLE STANDARD / BULK PUBLISHING
    # =================================================================================
    if not filenames:
        return jsonify({'success': False, 'message': 'No files provided'}), 400

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
                final_desc = req_description if req_description else (cached_data.get('description') or cached_data.get('Description', ''))
                print(f"DEBUG: Publishing {fname} with description: {final_desc}")
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
                    cursor.execute('INSERT INTO uploads (user_id, name, description, color_code, key_word, resolution, quality, category, link_small, link_medium, link_original, upload_date, ai_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', 
                                (session['user_id'], final_name, final_desc, final_color, final_keywords, item['resolution'], item['quality'], final_category, item['filename_small'], item['filename_medium'], item['filename_original'], time.time(), json.dumps(cached_data)))
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
        return jsonify({'success': False, 'message': 'Failed to process files: ' + '; '.join(errors)}), 500

# -------------------------------------------------------------------
# Serve File Route: Retrieves images from the uploads folder
# -------------------------------------------------------------------
@app.route('/uploads/<filename>')
def uploaded_file(filename):
    """Serves an uploaded file."""
    # 0. Proxy Mode (For Editor/Canvas CORS)
    if request.args.get('proxy') == '1' and s3_client and R2_BUCKET_NAME:
        try:
            file_obj = s3_client.get_object(Bucket=R2_BUCKET_NAME, Key=filename)
            return send_file(
                file_obj['Body'],
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
            t.name, t.description, t.color_code, t.key_word,
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
                res = row['resolution'] if 'resolution' in row.keys() else ""
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
            
            sql = f"""
                SELECT t.id, t.user_id, 
                t.name, t.description, t.color_code, t.key_word,
                t.resolution, t.quality, t.category, t.link_small, t.link_medium, t.link_original, t.upload_date, t.likes, t.views, t.downloads, t.ai_data,
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
                    SELECT t.id, t.user_id, 
                    t.name, t.description, t.color_code, t.key_word,
                    t.resolution, t.quality, t.category, t.link_small, t.link_medium, t.link_original, t.upload_date, t.likes, t.views, t.downloads, t.ai_data,
                    '{cat}' as category_type, u.username, u.avatar,
                    EXISTS(SELECT 1 FROM users_db.user_likes WHERE user_id = ? AND asset_id = t.id AND category = '{cat}') as is_liked,
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

@app.route('/search')
def search_page():
    """Renders the home page with search results pre-calculated for SEO."""
    query = request.args.get('q', '').strip()
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
    results = perform_search_logic(query, 'All', None, None, 'newest', user_id or 0)
    
    return render_template('home.html', 
                           has_unread=has_unread, 
                           theme_color=theme_color, 
                           theme_mode=theme_mode, 
                           r2_domain=R2_DOMAIN,
                           search_query=query,
                           result_count=len(results))

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
                           theme_mode=session.get('theme_mode', 'light'))

@app.route('/sitemap.xml')
def sitemap():
    """Generates an XML sitemap for Google Image SEO."""
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
            # Get latest 1000 items per category
            cursor.execute("SELECT id, name, link_small FROM uploads ORDER BY upload_date DESC LIMIT 1000")
            rows = cursor.fetchall()
            conn.close()
            
            for row in rows:
                # The Page URL (Landing Page)
                safe_slug = slugify(row[1])
                loc = f"{host}/view/{cat}/{row[0]}/{safe_slug}"
                # The Image URL (Direct File)
                img_loc = f"{R2_DOMAIN}/{row[2]}" if R2_DOMAIN else f"{host}/uploads/{row[2]}"
                
                xml.append('<url>')
                xml.append(f'<loc>{loc}</loc>')
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
    return Response('\n'.join(xml), mimetype='text/xml')

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
        session['user_id'] = result['user_id']
        session['username'] = result.get('username', username)
        session['role'] = result['role']
        session['avatar'] = result.get('avatar')
        return jsonify(result), 200
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
        return jsonify({'success': False, 'message': 'Authentication required'}), 401
    
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
            cursor.execute("UPDATE users SET password = ? WHERE id = ?", (hashed_pw, session['user_id']))
            
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
        return jsonify({'success': False, 'message': 'Authentication required'}), 401
    
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
        return jsonify({'success': False, 'message': 'Authentication required'}), 401
    
    data = request.get_json()
    asset_id = data.get('asset_id')
    category = data.get('category')
    user_id = session['user_id']
    
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
        return jsonify({'success': False, 'message': 'Authentication required'}), 401
    
    user_id = session['user_id']
    limit = int(request.args.get('limit', 20))
    offset = int(request.args.get('offset', 0))
    
    saved_assets = []
    fetch_limit = offset + limit
    
    for category, db_name in DB_MAPPING.items():
        try:
            conn = sqlite3.connect(db_name)
            conn.row_factory = sqlite3.Row
            conn.execute(f"ATTACH DATABASE '{USERS_DB}' AS users_db")
            cursor = conn.cursor()
            
            cursor.execute(f'''
                SELECT uploads.id, uploads.name, uploads.description, uploads.link_small, uploads.upload_date,
                uploads.likes, uploads.views, uploads.downloads,
                '{category}' as category_type, users.username, users.avatar,
                1 as is_saved,
                (SELECT 1 FROM users_db.user_likes WHERE user_id = ? AND asset_id = uploads.id AND category = ?) as is_liked
                FROM uploads
                JOIN users_db.user_saves s ON uploads.id = s.asset_id AND s.category = ? AND s.user_id = ?
                LEFT JOIN users_db.users users ON uploads.user_id = users.id
                ORDER BY upload_date DESC
                LIMIT ?
            ''', (user_id, category, category, user_id, fetch_limit))
            
            rows = cursor.fetchall()
            for row in rows:
                saved_assets.append(dict(row))
            conn.close()
        except Exception as e:
            print(f"Error fetching saved {category}: {e}")
            
    saved_assets.sort(key=lambda x: x['upload_date'], reverse=True)
    paginated_assets = saved_assets[offset : offset + limit]
    
    return jsonify({'success': True, 'assets': paginated_assets, 'has_more': len(saved_assets) > offset + limit})

@app.route('/api/user/uploads')
def get_user_uploads():
    if 'user_id' not in session:
        return jsonify({'success': False, 'message': 'Authentication required'}), 401
    
    user_id = session['user_id']
    limit = int(request.args.get('limit', 20))
    offset = int(request.args.get('offset', 0))
    
    user_uploads = []
    fetch_limit = offset + limit
    
    for category, db_name in DB_MAPPING.items():
        try:
            conn = sqlite3.connect(db_name)
            conn.row_factory = sqlite3.Row
            conn.execute(f"ATTACH DATABASE '{USERS_DB}' AS users_db")
            cursor = conn.cursor()
            
            cursor.execute(f'''
                SELECT uploads.id, uploads.name, uploads.description, uploads.link_small, uploads.upload_date,
                uploads.likes, uploads.views, uploads.downloads,
                '{category}' as category_type,
                (SELECT 1 FROM users_db.user_likes WHERE user_id = ? AND asset_id = uploads.id AND category = ?) as is_liked,
                (SELECT 1 FROM users_db.user_saves WHERE user_id = ? AND asset_id = uploads.id AND category = ?) as is_saved
                FROM uploads
                WHERE user_id = ?
                ORDER BY upload_date DESC
                LIMIT ?
            ''', (user_id, category, user_id, category, user_id, fetch_limit))
            
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
        return jsonify({'success': False, 'message': 'Authentication required'}), 401

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

        cursor.execute("SELECT user_id, link_small, link_medium, link_original FROM uploads WHERE id = ?", (asset_id,))
        asset = cursor.fetchone()

        if not asset:
            conn.close()
            return jsonify({'success': False, 'message': 'Asset not found'}), 404

        if asset['user_id'] != session['user_id']:
            conn.close()
            return jsonify({'success': False, 'message': 'Permission denied'}), 403

        files_to_delete = [asset['link_small'], asset['link_medium'], asset['link_original']]
        
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
        return jsonify({'success': False, 'message': 'Authentication required'}), 401
    
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
        return jsonify({'success': False, 'message': 'Authentication required'}), 401
    
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
    stats = {'users': 0, 'images': 0, 'logos': 0, 'downloads': 0}
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
                cursor.execute("SELECT link_original, link_medium, link_small FROM uploads")
                for row in cursor.fetchall():
                    registered_files.add(row[0])
                    if row[1]: registered_files.add(row[1])
                    if row[2]: registered_files.add(row[2])

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
                        # Determine base name for grouping
                        base_name = key
                        if key.startswith('small_'):
                            base_name = key[6:].rsplit('.', 1)[0]
                        elif key.startswith('medium_'):
                            base_name = key[7:].rsplit('.', 1)[0]
                        else:
                            base_name = key.rsplit('.', 1)[0]
                        
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
                    cursor.execute('INSERT INTO uploads (user_id, name, description, color_code, key_word, resolution, quality, category, link_small, link_medium, link_original, upload_date, ai_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', 
                                (user_id, name, description, color, keywords, item.get('resolution'), item.get('quality'), category, item.get('link_small'), item.get('link_medium'), item.get('link_original'), time.time(), ai_data_val))
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

