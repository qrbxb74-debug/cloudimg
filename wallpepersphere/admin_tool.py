import os
import json
import time
import uuid
import requests
import datetime
import boto3
import sqlite3
import xml.etree.ElementTree as ET
import re
from urllib.parse import urlparse
from PIL import Image
from dotenv import load_dotenv
from botocore.config import Config
from visual import VisualRecognizer
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Load environment variables
load_dotenv()

# Configuration
R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_BUCKET_NAME = os.environ.get('R2_BUCKET_NAME')
R2_DOMAIN = os.environ.get('R2_DOMAIN', '').rstrip('/')
ADMIN_TOKEN = os.environ.get('ADMIN_TOKEN')
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY')

# Target App URL (Change this to your live URL when deploying, e.g., https://myapp.onrender.com)
APP_API_URL = os.environ.get('APP_API_URL', "https://www.wallpepersphere.com/api/admin/bulk_import")

if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME, ADMIN_TOKEN, GOOGLE_API_KEY]):
    print("Error: Missing R2 credentials, ADMIN_TOKEN, or GOOGLE_API_KEY in .env file.")
    exit(1)

# Initialize R2 Client
s3_client = boto3.client(
    's3',
    endpoint_url=f'https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com',
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    config=Config(signature_version='s3v4'),
    region_name='auto'
)

# Initialize Gemini AI
recognizer = VisualRecognizer(api_key=GOOGLE_API_KEY)

# --- Indexing History Helpers ---
INDEXING_HISTORY_FILE = 'indexing_history.json'

def load_indexing_history():
    """Loads the list of already submitted URLs."""
    if os.path.exists(INDEXING_HISTORY_FILE):
        try:
            with open(INDEXING_HISTORY_FILE, 'r') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_indexing_history(history):
    """Saves the submitted URLs to a file."""
    try:
        with open(INDEXING_HISTORY_FILE, 'w') as f:
            json.dump(history, f, indent=2)
    except Exception as e:
        print(f"Warning: Could not save indexing history: {e}")

def slugify(text):
    """Converts text to a slug (e.g., 'Hello World!' -> 'hello-world')."""
    if not text: return "asset"
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s-]', '', text)
    text = re.sub(r'[\s-]+', '-', text)
    return text.strip('-')

def get_all_site_urls():
    """Fetches all asset URLs from the local databases."""
    urls = []
    # Parse base URL from the full API URL
    parsed = urlparse(APP_API_URL)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    # 1. Try to fetch from live Sitemap (Preferred)
    sitemap_url = f"{base_url}/sitemap.xml"
    print(f"Attempting to fetch links from: {sitemap_url}")
    
    try:
        response = requests.get(sitemap_url, timeout=10)
        if response.status_code == 200:
            # Parse XML
            root = ET.fromstring(response.content)
            # Handle XML Namespaces (Google sitemaps usually have one)
            namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            
            # Find all <loc> tags
            for url_tag in root.findall('.//ns:loc', namespace):
                if url_tag.text:
                    urls.append(url_tag.text.strip())
            
            print(f"✅ Successfully extracted {len(urls)} URLs from sitemap.xml")
            return urls
    except Exception as e:
        print(f"⚠️ Could not fetch live sitemap ({e}). Falling back to local database...")

    # 2. Fallback: Scan local databases
    db_folder = os.environ.get('DB_FOLDER', '.')
    db_mapping = {
        'image': os.path.join(db_folder, '1img.sql'),
        'logo': os.path.join(db_folder, '2logo.sql')
    }

    for category, db_path in db_mapping.items():
        if not os.path.exists(db_path):
            print(f"Warning: Database not found at {db_path}, skipping.")
            continue
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id, name FROM uploads")
                rows = cursor.fetchall()
                for row in rows:
                    asset_id, name = row
                    slug = slugify(name)
                    url = f"{base_url}/view/{category}/{asset_id}/{slug}"
                    urls.append(url)
        except Exception as e:
            print(f"Error reading from {db_path}: {e}")
    
    print(f"Found {len(urls)} total URLs to index.")
    return urls

def submit_urls_for_indexing(urls_to_submit, force=False):
    """Submits URLs to the Google Indexing API with history tracking and limits."""
    if not urls_to_submit:
        print("No URLs to submit.")
        return

    # 1. Load History & Calculate Quota
    history = load_indexing_history()
    now = time.time()
    DAILY_LIMIT = 200
    
    # Count submissions in the last 24 hours (86400 seconds)
    submitted_last_24h = [ts for ts in history.values() if ts > now - 86400]
    count_last_24h = len(submitted_last_24h)
    quota_left = max(0, DAILY_LIMIT - count_last_24h)

    # Filter pending URLs
    if force:
        pending_urls = urls_to_submit
    else:
        pending_urls = [u for u in urls_to_submit if u not in history]

    # 2. Display Info Board
    print("\n" + "="*45)
    print("       📊 GOOGLE INDEXING INFO BOARD")
    print("="*45)
    print(f" Total URLs in Database:    {len(urls_to_submit)}")
    print(f" Previously Indexed:        {len(urls_to_submit) - len(pending_urls)}")
    print(f" Pending Submission:        {len(pending_urls)}")
    print("-" * 45)
    print(f" Submitted in Last 24h:     {count_last_24h} / {DAILY_LIMIT}")
    print(f" Remaining Daily Quota:     {quota_left}")
    print("="*45 + "\n")

    # 3. Check Lockout / Nothing to do
    if not pending_urls and not force:
        print("✅ All URLs are up to date. No action needed.")
        return

    # Check for credentials
    creds_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    # Fallback: Check current directory if env var is missing
    if not creds_file and os.path.exists('service-account.json'):
        creds_file = 'service-account.json'
        
    if not creds_file or not os.path.exists(creds_file):
        print("❌ Error: GOOGLE_APPLICATION_CREDENTIALS not set or file not found.")
        print("Please set this in your .env file pointing to your service account JSON.")
        return

    if quota_left <= 0:
        # Calculate wait time
        if submitted_last_24h:
            oldest_submission = min(submitted_last_24h)
            unlock_time = oldest_submission + 86400
            wait_seconds = max(0, unlock_time - now)
            hours = int(wait_seconds // 3600)
            minutes = int((wait_seconds % 3600) // 60)
            print(f"⛔ SYSTEM LOCKED: Daily limit of {DAILY_LIMIT} URLs reached.")
            print(f"⏳ Please wait {hours}h {minutes}m for the quota to reset.")
        else:
            print("⛔ SYSTEM LOCKED: Daily limit reached.")
        return

    # 4. Prepare Batch
    batch_size = min(len(pending_urls), quota_left)
    batch = pending_urls[:batch_size]
    
    if not force:
        confirm = input(f"🚀 Ready to submit {len(batch)} URLs. Proceed? (y/n): ")
        if confirm.lower() != 'y':
            print("Operation cancelled.")
            return

    try:
        print("Authenticating with Google Indexing API...")
        credentials = service_account.Credentials.from_service_account_file(
            creds_file, scopes=["https://www.googleapis.com/auth/indexing"]
        )
        service = build("indexing", "v3", credentials=credentials)
        
        success_count = 0
        
        # 5. Submit Batch
        for i, url in enumerate(batch):
            try:
                content = {"url": url, "type": "URL_UPDATED"}
                response = service.urlNotifications().publish(body=content).execute()
                
                # Extract timestamp for visual confirmation
                notify_time = response.get('urlNotificationMetadata', {}).get('latestUpdate', {}).get('notifyTime', 'Unknown')
                print(f"[{i+1}/{len(batch)}] ✅ Success: {url} \n       (Google Timestamp: {notify_time})")
                
                # Update history immediately
                history[url] = time.time()
                success_count += 1
                
                # Small delay to be nice to the API
                time.sleep(0.5)
                
            except HttpError as e:
                print(f"[{i+1}/{len(batch)}] Failed: {e}")
            except Exception as e:
                print(f"[{i+1}/{len(batch)}] Error: {e}")

        # 4. Save History
        save_indexing_history(history)
        print(f"\n✅ Batch complete. {success_count} URLs submitted.")
            
    except Exception as e:
        print(f"❌ Authentication or Service Error: {e}")

def check_url_status(url):
    """Checks the notification status of a URL using Google Indexing API."""
    # Check for credentials
    creds_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if not creds_file and os.path.exists('service-account.json'):
        creds_file = 'service-account.json'
        
    if not creds_file or not os.path.exists(creds_file):
        print("❌ Error: GOOGLE_APPLICATION_CREDENTIALS not set or file not found.")
        return

    try:
        credentials = service_account.Credentials.from_service_account_file(
            creds_file, scopes=["https://www.googleapis.com/auth/indexing"]
        )
        service = build("indexing", "v3", credentials=credentials)
        
        print(f"🔍 Checking status for: {url} ...")
        response = service.urlNotifications().getMetadata(url=url).execute()
        
        print("\n=== GOOGLE INDEXING STATUS ===")
        print(json.dumps(response, indent=2))
        print("==============================\n")
        
        latest = response.get('latestUpdate', {})
        if latest and latest.get('notifyTime'):
            notify_time_str = latest.get('notifyTime')
            # Parse RFC 3339 timestamp (e.g., 2023-10-27T10:00:00Z)
            try:
                # Handle Z for UTC
                notify_time_str = notify_time_str.replace('Z', '+00:00')
                notify_dt = datetime.datetime.fromisoformat(notify_time_str)
                now_dt = datetime.datetime.now(datetime.timezone.utc)
                diff = now_dt - notify_dt
                
                hours = int(diff.total_seconds() // 3600)
                minutes = int((diff.total_seconds() % 3600) // 60)
                
                print(f"✅ CONFIRMED: Google received your request {hours}h {minutes}m ago.")
                print(f"   Type: {latest.get('type')}")
                print(f"   Status: The ball is in Google's court. Crawling usually happens within 24h.")
            except Exception:
                print(f"✅ CONFIRMED: Google received your request at {notify_time_str}")

            # Generate a direct search link
            print(f"\n🔎 Live Check Link: https://www.google.com/search?q=site:{url}")
        else:
            print("⚠️ NOT FOUND: Google has NO record of receiving this URL via the API.")
            print("   Action: Please submit this URL using Option 3.")

    except HttpError as e:
        print(f"❌ API Error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

def upload_file(file_path, object_name):
    """Uploads a file to R2."""
    try:
        content_type = 'image/jpeg'
        if object_name.endswith('.webp'): content_type = 'image/webp'
        elif object_name.endswith('.png'): content_type = 'image/png'
        
        s3_client.upload_file(file_path, R2_BUCKET_NAME, object_name, ExtraArgs={'ContentType': content_type})
        return f"{R2_DOMAIN}/{object_name}"
    except Exception as e:
        print(f"Upload failed: {e}")
        return None

def process_images(folder_path):
    """
    1. Resizes images (Small, Medium).
    2. Uploads to R2.
    3. Analyzes with Gemini AI.
    4. Returns a list of complete asset objects ready for DB import.
    """
    final_payload = []
    
    print(f"\n--- Processing Images in {folder_path} ---")
    
    files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.png', '.jpg', '.jpeg', '.webp'))]
    
    if not files:
        print("No images found in folder.")
        return []

    def compress_and_save(image, path, target_kb):
        """Compresses image to target size in KB."""
        quality = 90
        step = 5
        min_quality = 5
        image.save(path, 'WEBP', quality=quality)
        while os.path.getsize(path) > target_kb * 1024 and quality > min_quality:
            quality -= step
            image.save(path, 'WEBP', quality=quality)

    for filename in files:
        src_path = os.path.join(folder_path, filename)
        unique_id = uuid.uuid4().hex[:8]
        timestamp = int(time.time())
        
        # Define filenames
        base_name = f"bulk_{timestamp}_{unique_id}"
        name_original = f"{base_name}_original.jpg"
        name_medium = f"medium_{base_name}.webp"
        name_small = f"small_{base_name}.webp"
        name_tiny = f"tiny_{base_name}.webp"
        
        try:
            with Image.open(src_path) as img:
                if img.mode in ('RGBA', 'P'): img = img.convert('RGB')
                
                # Calculate Quality/Resolution
                width, height = img.size
                resolution = f"{width}x{height}"
                long_side = max(width, height)
                if long_side >= 3840: quality = '4K'
                elif long_side >= 2560: quality = '2K'
                elif long_side >= 1920: quality = 'FHD'
                elif long_side >= 1280: quality = 'HD'
                else: quality = 'SD'

                # Save Temp Files
                temp_medium = f"temp_{name_medium}"
                temp_small = f"temp_{name_small}"
                temp_tiny = f"temp_{name_tiny}"
                
                # Medium
                img_copy = img.copy()
                img_copy.thumbnail((2048, 2048))
                compress_and_save(img_copy, temp_medium, 100)
                
                # Small
                img_copy = img.copy()
                img_copy.thumbnail((1024, 1024))
                compress_and_save(img_copy, temp_small, 40)

                # Tiny
                img_copy = img.copy()
                img_copy.thumbnail((400, 400))
                compress_and_save(img_copy, temp_tiny, 10)
                
                print(f"Uploading {filename}...")
                
                # Upload to R2
                link_original = upload_file(src_path, name_original) # Upload original source as jpg/png
                link_medium = upload_file(temp_medium, name_medium)
                link_small = upload_file(temp_small, name_small)
                link_tiny = upload_file(temp_tiny, name_tiny)
                
                # Analyze with Gemini (Using local source file)
                print(f" -> Analyzing with Gemini AI...")
                ai_result = recognizer.analyze_image(src_path)
                
                ai_data = {}
                if ai_result['success']:
                    ai_data = ai_result['data']
                    print(f" -> AI Analysis Success: {ai_data.get('name')}")
                else:
                    print(f" -> AI Analysis Failed: {ai_result.get('error')}")
                    # Fallback data
                    ai_data = {
                        'name': filename.split('.')[0].replace('-', ' ').title(),
                        'description': 'Uploaded via Admin Tool',
                        'keywords': 'wallpaper, 4k, hd',
                        'color': 'Unknown',
                        'category': 'Abstract'
                    }

                # Cleanup Temp
                os.remove(temp_medium)
                os.remove(temp_small)
                os.remove(temp_tiny)
                
                if link_original and link_medium:
                    final_payload.append({
                        'name': ai_data.get('name'),
                        'description': ai_data.get('description'),
                        'keywords': ai_data.get('keywords'),
                        'color': ai_data.get('color'),
                        'category': ai_data.get('category'),
                        'link_original': name_original,
                        'link_medium': name_medium,
                        'link_small': name_small,
                        'link_tiny': name_tiny,
                        'resolution': resolution,
                        'quality': quality,
                        'user_id': 1 # Default to Admin
                    })
                else:
                    print(" -> Upload Failed")

        except Exception as e:
            print(f" -> Error processing {filename}: {e}")

    return final_payload

def check_server_connection():
    """Checks if the target server is reachable before starting heavy processing."""
    try:
        # Extract base URL (e.g., https://www.wallpepersphere.com)
        from urllib.parse import urlparse
        parsed = urlparse(APP_API_URL)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        print(f"Checking connection to {base_url}...")
        # Use a browser User-Agent to avoid Cloudflare blocking the script
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        response = requests.get(base_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            print("✅ Server is online.")
            return True
        else:
            print(f"⚠️ Server returned status code: {response.status_code}")
            return True
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        print("Please check:")
        print("1. Your internet connection.")
        print("2. The APP_API_URL in your .env file.")
        print("3. If your site is currently running.")
        return False

def main():
    print("=== ADMIN BULK UPLOADER & INDEXING TOOL ===")
    
    # Check connection first
    if not check_server_connection():
        retry = input("Connection failed. Continue anyway? (y/n): ")
        if retry.lower() != 'y':
            return
    
    print("\nSelect an option:")
    print("1. Bulk Upload Images from a Folder")
    print("2. Submit All Site URLs to Google Indexing API")
    print("3. Manually Submit a Specific URL")
    print("4. Check Indexing Status of a URL")
    choice = input("Choice: ").strip()

    if choice == '1':
        print("\nEnter the full path to your local image folder.")
        folder = input("Folder Path: ").strip().strip('"')
        
        if not os.path.exists(folder):
            print("Folder does not exist.")
            return

        # Step 1: Process, Upload & Analyze (All-in-one)
        final_payload = process_images(folder)

        if not final_payload:
            print("No valid data to send.")
            return

        # Step 2: Send to App
        print(f"\nSending {len(final_payload)} items to {APP_API_URL}...")
        
        try:
            headers = {
                'Content-Type': 'application/json',
                'X-Admin-Token': ADMIN_TOKEN
            }
            response = requests.post(APP_API_URL, json=final_payload, headers=headers)
            
            if response.status_code == 200:
                res_json = response.json()
                print("\nSUCCESS!")
                print(f"Imported: {res_json.get('imported')}")
                if res_json.get('errors'):
                    print("Errors:", res_json['errors'])
            else:
                print(f"Failed. Status: {response.status_code}")
                print(response.text)
                
        except Exception as e:
            print(f"Connection Error: {e}")
            print("Make sure your app is running and the URL is correct.")
            
    elif choice == '2':
        print("\n--- Google Indexing API Submission ---")
        urls = get_all_site_urls()
        if urls:
            submit_urls_for_indexing(urls)
    elif choice == '3':
        print("\nEnter the full URL you want to index (e.g., https://wallpepersphere.com/view/image/123):")
        url = input("URL: ").strip()
        if url:
            # Fix: Ensure URL has scheme
            if not url.startswith(('http://', 'https://')):
                url = f'https://{url}'
            submit_urls_for_indexing([url], force=True)
    elif choice == '4':
        print("\nEnter the full URL to check status:")
        url = input("URL: ").strip()
        if url:
            if not url.startswith(('http://', 'https://')):
                url = f'https://{url}'
            check_url_status(url)
    else:
        print("Invalid choice.")

if __name__ == "__main__":
    main()
