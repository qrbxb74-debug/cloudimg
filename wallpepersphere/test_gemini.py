import os
import logging
from dotenv import load_dotenv
from gemini_generator import GeminiGenerator

# Mock logging for console
logging.basicConfig(level=logging.INFO)

def test_gemini():
    load_dotenv()
    api_key = os.environ.get('GEMINI_API_KEY') or os.environ.get('GOOGLE_API_KEY')
    print(f"Testing with API Key: {api_key[:10]}...")
    
    gen = GeminiGenerator(api_key, "./temp_uploads")
    if not os.path.exists("./temp_uploads"):
        os.makedirs("./temp_uploads")
        
    result = gen.generate_image("A test image of a futuristic banana", aspect_ratio="1:1", user_id="test_user")
    print("Result:", result)

if __name__ == "__main__":
    test_gemini()
