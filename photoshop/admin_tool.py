import os
import json
import time
import uuid
import requests
import boto3
from PIL import Image
from dotenv import load_dotenv
from botocore.config import Config

# Load environment variables
load_dotenv()

# Configuration
R2_ACCOUNT_ID = os.environ.get('R2_ACCOUNT_ID')
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_BUCKET_NAME = os.environ.get('R2_BUCKET_NAME')
R2_DOMAIN = os.environ.get('R2_DOMAIN', '').rstrip('/')
ADMIN_TOKEN = os.environ.get('ADMIN_TOKEN')

# Target App URL (Change this to your live URL when deploying, e.g., https://myapp.onrender.com)
APP_API_URL = os.environ.get('APP_API_URL', "http://127.0.0.1:5000/api/admin/bulk_import")

if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME, ADMIN_TOKEN]):
    print("Error: Missing R2 credentials or ADMIN_TOKEN in .env file.")
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
    2. Uploads Original, Small, Medium to R2.
    3. Returns a list of file data objects.
    """
    processed_files = []
    
    print(f"\n--- Processing Images in {folder_path} ---")
    
    files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.png', '.jpg', '.jpeg', '.webp'))]
    
    if not files:
        print("No images found in folder.")
        return []

    for filename in files:
        src_path = os.path.join(folder_path, filename)
        unique_id = uuid.uuid4().hex[:8]
        timestamp = int(time.time())
        
        # Define filenames
        base_name = f"bulk_{timestamp}_{unique_id}"
        name_original = f"{base_name}_original.jpg"
        name_medium = f"medium_{base_name}.webp"
        name_small = f"small_{base_name}.webp"
        
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
                
                # Medium
                img_copy = img.copy()
                img_copy.thumbnail((2048, 2048))
                img_copy.save(temp_medium, 'WEBP', quality=90)
                
                # Small
                img_copy = img.copy()
                img_copy.thumbnail((1024, 1024))
                img_copy.save(temp_small, 'WEBP', quality=80)
                
                print(f"Uploading {filename}...")
                
                # Upload to R2
                link_original = upload_file(src_path, name_original) # Upload original source as jpg/png
                link_medium = upload_file(temp_medium, name_medium)
                link_small = upload_file(temp_small, name_small)
                
                # Cleanup Temp
                os.remove(temp_medium)
                os.remove(temp_small)
                
                if link_original and link_medium:
                    processed_files.append({
                        'temp_id': filename, # Used to match user mind
                        'link_original': name_original,
                        'link_medium': name_medium,
                        'link_small': name_small,
                        'public_url': link_small, # Send small to ChatGPT to save bandwidth
                        'resolution': resolution,
                        'quality': quality
                    })
                    print(f" -> Success: {quality} {resolution}")
                else:
                    print(" -> Upload Failed")

        except Exception as e:
            print(f" -> Error processing {filename}: {e}")

    return processed_files

def main():
    print("=== ADMIN BULK UPLOADER TOOL ===")
    print("1. Enter the full path to your local image folder.")
    folder = input("Folder Path: ").strip().strip('"')
    
    if not os.path.exists(folder):
        print("Folder does not exist.")
        return

    # Step 1: Process & Upload
    assets = process_images(folder)
    if not assets: return

    # Step 2: Generate ChatGPT Prompt
    print("\n" + "="*60)
    print("STEP 2: COPY THIS PROMPT TO CHATGPT")
    print("="*60)
    
    links_list = [a['public_url'] for a in assets]
    
    prompt = f"""
I have a list of images. Please analyze them and return a JSON array.
For each image, provide:
- "name": A creative title (4-6 words)
- "description": A short description (10-15 words)
- "keywords": A string of comma-separated keywords (10-15 keywords)
- "color": The dominant color name
- "category": Choose best from [Abstract, Animals, Anime, Architecture, Art, Cars, City, Cyberpunk, Fantasy, Gaming, Nature, Neon, Space, Technology]
- "url": The exact URL provided below (so I can match the data back)

Here are the image links:
{json.dumps(links_list, indent=2)}

Return ONLY the valid JSON array. No markdown, no text.
"""
    print(prompt)
    print("="*60)
    
    # Step 3: Receive JSON
    print("\nSTEP 3: PASTE THE JSON RESPONSE FROM CHATGPT BELOW.")
    print("Type 'END' on a new line when finished pasting.")
    
    json_lines = []
    while True:
        line = input()
        if line.strip() == 'END':
            break
        json_lines.append(line)
    
    raw_json = "\n".join(json_lines)
    
    try:
        # Clean up markdown code blocks if user pasted them
        raw_json = raw_json.replace('```json', '').replace('```', '').strip()
        ai_data = json.loads(raw_json)
    except json.JSONDecodeError:
        print("CRITICAL: Invalid JSON pasted. Please try again.")
        return

    # Step 4: Merge Data
    final_payload = []
    
    # Create a lookup map for our local assets based on the public URL
    asset_map = {a['public_url']: a for a in assets}
    
    for item in ai_data:
        url = item.get('url')
        local_asset = asset_map.get(url)
        
        if local_asset:
            # Merge AI data with our R2 file paths
            payload_item = {
                'name': item.get('name'),
                'description': item.get('description'),
                'keywords': item.get('keywords'),
                'color': item.get('color'),
                'category': item.get('category'),
                'link_original': local_asset['link_original'],
                'link_medium': local_asset['link_medium'],
                'link_small': local_asset['link_small'],
                'resolution': local_asset['resolution'],
                'quality': local_asset['quality'],
                'user_id': 1 # Default to Admin ID
            }
            final_payload.append(payload_item)
        else:
            print(f"Warning: Could not match AI result for URL: {url}")

    if not final_payload:
        print("No valid data to send.")
        return

    # Step 5: Send to App
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

if __name__ == "__main__":
    main()
