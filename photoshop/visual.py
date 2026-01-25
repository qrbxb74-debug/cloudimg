import os
import json
import re
import time
from google import genai
from google.genai import types
from PIL import Image

CATEGORIES = [
"Abstract", "Aesthetic", "AI Art", "Airplanes", "Animals", "Anime", 
"Architecture", "Art", "Astronomy", "Backgrounds", "Beach", 
"Biology", "Business", "Cars", "Cartoons", "Celebrities", 
"City", "Cityscape", "Clouds", "Computers", "Concept Art", 
"Creative", "Cyberpunk", "Dark", "Design", "Digital Art", 
"Education", "Fantasy", "Fashion", "Film", "Flowers", 
"Food", "Forest", "Futuristic", "Gaming", "Geometric", 
"Gradients", "Graphics", "Health", "Holidays", "Home", 
"Icons", "Illustrations", "Industrial", "Interiors", 
"Landscape Photography", "Landscapes", "Lifestyle", "Love", 
"Macro", "Minimal", "Mountains", "Music", "Nature", 
"Neon", "Night", "Ocean", "Patterns", "People", 
"Pets", "Photography", "Portraits", "Quotes", 
"Retro", "Robotics", "Sci-Fi", "Seasons", "Sky", 
"Social Media", "Space", "Sports", "Street", 
"Street Photography", "Surreal", "Technology", 
"Textures", "Time-lapse", "Travel", "Typography", 
"Underwater", "Urban", "Vector", "Vehicles", 
"Vintage", "Water", "Waterfalls", "Weather", 
"Wildlife", "Winter", "Woods", "Zen"

]

class VisualRecognizer:
    def __init__(self, api_keys=None, api_key=None):
        """
        Initializes the Gemini Visual Recognizer.
        Supports multiple API keys for rotation to extend free tier limits.
        """
        self.api_keys = []
        
        # Handle list of keys
        if api_keys and isinstance(api_keys, list):
            self.api_keys.extend([k for k in api_keys if k])
            
        # Handle single key argument (legacy support)
        if api_key:
            self.api_keys.append(api_key)
            
        # Fallback to environment variable if list is empty
        if not self.api_keys:
            key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
            if key:
                self.api_keys.append(key)
        
        # Remove duplicates
        self.api_keys = list(dict.fromkeys(self.api_keys))

        if not self.api_keys:
            print("Warning: No API keys provided.")

        self.current_key_index = 0
        self.request_count = 0
        self.rotation_threshold = 4 # Rotate after 4 requests
        self.client = None
        
        self._init_client()

    def _init_client(self):
        """Initializes the GenAI client with the current active key."""
        if not self.api_keys: return
        
        current_key = self.api_keys[self.current_key_index]
        # print(f"Initializing Gemini Client with Key Index {self.current_key_index}")
        self.client = genai.Client(api_key=current_key)
        self.model_name = self._get_valid_model()

    def _get_valid_model(self):
        """
        Dynamically finds a valid model, prioritizing Gemini 1.5 Flash.
        Adapted from the working test script to prevent 404 errors.
        """
        try:
            # Get all models
            models = list(self.client.models.list())
            model_names = [m.name for m in models if 'generateContent' in m.supported_actions]
            
            # Priority list of models to look for
            priorities = [
                'gemini-1.5-flash',
                'gemini-1.5-flash-8b',
                'gemini-1.5-flash-001',
                'gemini-1.5-flash-002',
                'gemini-1.5-pro'
            ]
            
            for p in priorities:
                for m_name in model_names:
                    if p in m_name:
                        return m_name
            
            # Fallback: Use the first available model
            if model_names:
                return model_names[0]
        except Exception as e:
            print(f"Error listing models: {e}")
        
        # Ultimate fallback if listing fails
        return 'gemini-1.5-flash-001'

    def _rotate_key(self):
        """Switches to the next available API key."""
        if self.api_keys and len(self.api_keys) > 1:
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            self.request_count = 0
            print(f"[System] Switching to API Key Index {self.current_key_index}")
            self._init_client()

    def analyze_image(self, image_path):
        """
        Analyzes an image to extract description, components, actions, keywords, and color.
        Returns a dictionary with the analysis data.
        """
        # Rotation Logic: Switch key if threshold reached
        if self.api_keys and len(self.api_keys) > 1:
            if self.request_count >= self.rotation_threshold:
                self._rotate_key()

        if not self.client:
            return {"success": False, "error": "Gemini Client not initialized. Check API Keys."}

        if not os.path.exists(image_path):
            return {"success": False, "error": "File not found"}

        keys_tried_in_session = 0

        while True:
            img = None
            try:
                img = Image.open(image_path)
                
                # We construct a prompt to force the AI to return the specific JSON structure you need.
                prompt = f"""
                Analyze this image. Select the best category from: {", ".join(CATEGORIES)}
                
                Return a valid JSON object. Do not use Markdown.
                Structure:
                {{
                    "category": "Selected Category",
                    "name": "Title dont make it to long 4 to 6 words",
                    "description": "description dont make it too long 10 to 15 words",
                    "keywords": "k1 k2...",
                    "color": "Color"
                }}
                """

                # Set safety settings to avoid blocking harmless image analysis
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=[prompt, img],
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        max_output_tokens=4000,
                        safety_settings=[
                            types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="BLOCK_ONLY_HIGH"),
                            types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="BLOCK_ONLY_HIGH"),
                            types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="BLOCK_ONLY_HIGH"),
                            types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="BLOCK_ONLY_HIGH")
                        ]
                    )
                )
                
                # Check for safety violations
                candidate = response.candidates[0] if response.candidates else None
                if candidate and str(candidate.finish_reason) == "SAFETY":
                    # ... (Safety handling logic remains same, omitted for brevity but kept in flow) ...
                    safety_reasons = []
                    if candidate.safety_ratings:
                        for rating in candidate.safety_ratings:
                            if str(rating.probability) in ["HIGH", "MEDIUM"]:
                                safety_reasons.append(f"{rating.category}: {rating.probability}")
                    feedback = ", ".join(safety_reasons) if safety_reasons else "Unspecified Safety Violation"
                    
                    img.close(); img = None
                    if os.path.exists(image_path):
                        try: os.remove(image_path)
                        except: pass
                    return {"success": False, "error": f"SECURITY ALERT: {feedback}", "critical_stop": True}
                
                # Clean and parse the response
                text_content = response.text.strip()
                match = re.search(r'\{.*\}', text_content, re.DOTALL)
                if match:
                    try:
                        data = json.loads(match.group(0))
                        
                        # Normalize keys to lowercase to ensure 'description' is found even if AI returns 'Description'
                        if isinstance(data, dict):
                            data = {k.lower(): v for k, v in data.items()}

                        print(f"[DEBUG] Parsed JSON Data for {os.path.basename(image_path)}")
                        self.request_count += 1 # Increment only on success
                        return {"success": True, "data": data}
                    except json.JSONDecodeError as je:
                        print(f"[Error] JSON Decode Failed: {je}")
                        return {"success": False, "error": f"JSON Error: {je}"}
                else:
                    return {"success": False, "error": "No JSON found in response"}

            except Exception as e:
                error_str = str(e)
                # Check for Quota/Rate Limit errors (429) or Model Not Found (404)
                if any(err in error_str for err in ["429", "ResourceExhausted", "Quota", "Too Many Requests", "404", "Not Found", "503", "Overloaded", "UNAVAILABLE"]):
                    print(f"[Warning] API Issue ({error_str}) on Key Index {self.current_key_index}. Rotating...")
                    self._rotate_key()
                    keys_tried_in_session += 1
                    
                    # If we have tried all keys, sleep for 2 hours
                    if keys_tried_in_session >= max(1, len(self.api_keys)):
                        # Try to parse retry time from error message
                        retry_seconds = 60 # Default to 1 minute
                        match = re.search(r'retry in (\d+(\.\d+)?)s', error_str)
                        if match:
                            retry_seconds = float(match.group(1)) + 2.0 # Add 2s buffer
                        
                        print(f"[System] All API keys exhausted. Sleeping for {retry_seconds:.2f} seconds...")
                        if img: img.close(); img = None
                        time.sleep(retry_seconds)
                        print("[System] Waking up. Resuming processing...")
                        keys_tried_in_session = 0 # Reset session count
                    
                    continue # Retry the loop immediately
                else:
                    print(f"Error analyzing image: {e}")
                    return {"success": False, "error": str(e)}
            finally:
                if img:
                    img.close()
