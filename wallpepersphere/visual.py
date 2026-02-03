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
        Optimized for a single paid API key (No rotation).
        """
        self.api_key = None
        
        # Handle single key argument
        if api_key:
            self.api_key = api_key
        # Handle list of keys (Take the first one)
        elif api_keys and isinstance(api_keys, list) and len(api_keys) > 0:
            self.api_key = api_keys[0]
            
        # Fallback to environment variable
        if not self.api_key:
            self.api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")

        if not self.api_key:
            print("Warning: No API key provided.")

        self.client = None
        
        self._init_client()

    def _init_client(self):
        """Initializes the GenAI client with the active key."""
        if not self.api_key: return
        
        # print(f"Initializing Gemini Client...")
        self.client = genai.Client(api_key=self.api_key)
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

    def analyze_image(self, image_path):
        """
        Analyzes an image to extract description, components, actions, keywords, and color.
        Returns a dictionary with the analysis data.
        """
        if not self.client:
            return {"success": False, "error": "Gemini Client not initialized. Check API Key."}

        if not os.path.exists(image_path):
            return {"success": False, "error": "File not found"}

        img = None
        try:
            img = Image.open(image_path)
            
            # We construct a prompt to force the AI to return the specific JSON structure you need.
            prompt = f"""
            Analyze this image. Select the best category from: {", ".join(CATEGORIES)}
            
            Return a valid JSON object. Do not use Markdown.
            IMPORTANT: Analyze the actual image pixels to provide a good visual description. Do not hallucinate based on filename.
            Structure:
            {{
                "category": "Selected Category from the list",
                "name": "A creative title, don't make it too long (4 to 6 words)",
                "description": "A short, engaging description, don't make it too long (10 to 15 words)",
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
                    return {"success": True, "data": data}
                except json.JSONDecodeError as je:
                    print(f"[Error] JSON Decode Failed: {je}")
                    return {"success": False, "error": f"JSON Error: {je}"}
            else:
                return {"success": False, "error": "No JSON found in response"}

        except Exception as e:
            print(f"Error analyzing image: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if img:
                img.close()
