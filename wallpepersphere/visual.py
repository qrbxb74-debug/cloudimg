import os
import json
import re
import time
from google import genai
from google.genai import types
from PIL import Image

CATEGORIES = [
   "3D Art", "4D Art", "4K", "8K",
    "Abstract", "Abstract Nature", "Action", "Aesthetic", "Aesthetic Rooms",
    "Afrofuturism", "Ageing", "AI Art", "AI Generated", "Air",
    "Airbrush", "Airplanes", "Album Covers", "Alchemy", "Alien Architecture",
    "Alien Worlds", "Ambient", "Amoled", "Analog", "Ancient Civilizations",
    "Ancient Egypt", "Ancient Rome", "Android", "Angel", "Animals",
    "Anime", "Anime Boys", "Anime Girls", "Anime Landscapes", "Apex Legends",
    "Apocalyptic", "Aquatic", "Arabian Nights", "Arcane", "Architecture",
    "Archways", "Arctic", "Armor", "Art", "Art Deco",
    "Art Nouveau", "Astral", "Astronauts", "Astronomy", "Atmospheric",
    "Attack on Titan", "Aurora", "Autumn", "Avatar", "Aztec",
    "Baby Animals", "Backgrounds", "Bamboo", "Baroque", "Basketball",
    "Batik", "Battlefield", "Beach", "Bioluminescence", "Biology",
    "Black", "Black and White", "Black Hole", "Blade Runner", "Bloodborne",
    "Bloom", "Blue", "Blue Hour", "Bokeh", "Botanical",
    "Brands", "Bridges", "Brown", "Brutalist", "Buddha",
    "Business", "Butterfly", "Call of Duty", "Camo", "Canyon",
    "Cars", "Cartoons", "Cave", "Celebrities", "Celestial",
    "Cherry Blossom", "Chess", "Chinese Art", "Chrome", "Chromatic",
    "Chrysanthemum", "Cinematic", "Cinematic Portraits", "City", "Cityscape",
    "Claymation", "Clouds", "Clouds at Night", "Cloudpunk", "Coding",
    "Coffee", "Cold", "Colorful", "Colors", "Columns",
    "Comet", "Comic Book", "Computers", "Concept Art", "Constellation",
    "Copper", "Coral Reef", "Cosplay", "Cosmos", "Cottagecore",
    "Cracked", "Creative", "Creativity", "Crimson", "Crystal",
    "Cybercity", "Cybernetic", "Cyberpunk", "Cyberspace", "Daemon",
    "Dark", "Dark Academia", "Dark Fantasy", "Dawn", "DC Comics",
    "Death Note", "Debris", "Deep Sea", "Demon Slayer", "Depth",
    "Desert", "Design", "Devil", "Diamonds", "Digital Art",
    "Dimly Lit", "Dinosaurs", "Dojo", "Doodle", "Dragon",
    "Dragon Ball Z", "Dusk", "Dystopian", "Earth", "Eclipse",
    "Education", "Electric", "Eldritch", "Elven", "Emerald",
    "Enchanted", "Energy", "Epic", "Ethereal", "Explosion",
    "Extraterrestrial", "Fairy", "Fairy Tale", "Fallen Angel", "Fantasy",
    "Fashion", "Fern", "Festival", "Fiber Optic", "Film",
    "Filmgrain", "Fire", "Floral", "Flowers", "Fluid",
    "Fog", "Folklore", "Food", "Forest", "Fortnite",
    "Fractal", "Futuristic", "Futuristic City", "Gaming", "Geometric",
    "Ghibli", "Glitch Art", "Google Pixel", "Gothic", "Gradients",
    "Grand Theft Auto (GTA)", "Graphics", "Green", "Grey", "Grunge",
    "HD", "Health", "Holographic", "Holidays", "Home",
    "Horror", "Icons", "Illustrations", "Impressionism", "Industrial",
    "Infrared", "Interiors", "iPhone", "Jungle", "K-Pop",
    "Landscape Photography", "Landscapes", "Lifestyle", "Logos", "Love",
    "MacBook", "Macro", "Marvel", "Minimal", "Minecraft",
    "Movies", "Mountains", "Music", "Music Artists", "Naruto",
    "Nature", "Neon", "Night", "Ocean", "One Piece",
    "Orange", "Patterns", "People", "Pets", "Photography",
    "Pink", "Portraits", "Purple", "Quotes", "Red",
    "Retro", "Robotics", "Samsung", "Sci-Fi", "Seasons",
    "Sky", "Social Media", "Space", "Sports", "Sports Cars",
    "Star Wars", "Street", "Street Photography", "Superheroes", "Surreal",
    "Technology", "Textures", "Time-lapse", "Travel", "TV Shows",
    "Typography", "Underwater", "Urban", "Vector", "Vehicles",
    "Video Games", "Vintage", "Water", "Waterfalls", "Weather",
    "White", "Wildlife", "Winter", "Woods", "Yellow",
    "Zen"
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
        if not self.api_key:
            self.client = None
            self.model_name = None
            return

        try:
            print("Initializing Gemini Client...")
            self.client = genai.Client(api_key=self.api_key)
            self.model_name = self._get_valid_model()  # This will now return None on failure
            if not self.model_name:
                print("CRITICAL: Could not find a valid Gemini model. VisualRecognizer will be disabled.")
                self.client = None  # Disable client if no model is found
        except Exception as e:
            print(f"CRITICAL: Failed to initialize Gemini Client: {e}")
            self.client = None
            self.model_name = None

    def _get_valid_model(self):
        """
        Dynamically finds a valid model, prioritizing Gemini 1.5 Flash.
        Returns the model name string or None if no suitable model is found.
        """
        if not self.client:
            return None

        print("Searching for available Gemini models...")
        try:
            # Filter for models that support 'generateContent'
            model_names = [m.name for m in self.client.models.list() if 'generateContent' in m.supported_actions]

            if not model_names:
                print("Warning: No models found supporting 'generateContent'.")
                return None

            print(f"Found {len(model_names)} compatible models: {model_names}")

            # Priority list of models to look for
            priorities = [
                'gemini-1.5-flash-latest',  # Use 'latest' alias
                'gemini-1.5-flash',
                'gemini-1.5-pro-latest',
                'gemini-1.5-pro',
                'gemini-pro-vision'  # Older but stable
            ]

            for p in priorities:
                for m_name in model_names:
                    # Check for exact match or alias match (ignoring models/ prefix)
                    clean_name = m_name.split('/')[-1]
                    if p == clean_name:
                        print(f"✅ Selected priority model: {m_name}")
                        return m_name

            # Fallback: Use the first available model if no priority model was found
            if model_names:
                print(f"⚠️ No priority model found. Using first available: {model_names[0]}")
                return model_names[0]
        except Exception as e:
            print(f"❌ Error listing models: {e}. This might be an API key or permission issue.")
            return None

    def analyze_image(self, image_path):
        """
        Analyzes an image to extract description, components, actions, keywords, and color.
        Returns a dictionary with the analysis data.
        """
        if not self.client or not self.model_name:
            return {"success": False, "error": "Gemini Client not initialized or no model found. Check API Key and permissions."}

        if not os.path.exists(image_path):
            return {"success": False, "error": "File not found"}

        img = None
        try:
            img = Image.open(image_path)
            
            # We construct a prompt to force the AI to return the specific JSON structure you need.
            prompt = f"""
            Analyze this image to identify its main subject and visual characteristics.
            Select the best category for the image from the following list: {", ".join(CATEGORIES)}.

            Your response must be a valid JSON object, without any Markdown formatting.

            IMPORTANT:
            - The 'name' should be a concise and factual title representing the primary subject of the image (e.g., 'iPhone 17', 'Race Car', 'Eiffel Tower').
            - The 'description' should provide a brief visual summary of the image.
            - Do not invent details or base your analysis on the filename. Analyze the image's actual content.

            JSON Structure:
            {{
                "category": "Selected category from the provided list",
                "name": "The main subject of the image (e.g., 'iPhone 17', 'race car')",
                "description": "A short, engaging description of the visual elements (10 to 15 words)",
                "keywords": "k1 k2...",
                "color": "The dominant color in the image"
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
