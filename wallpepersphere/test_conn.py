import os
from dotenv import load_dotenv
from google import genai

def test_connectivity():
    load_dotenv()
    api_key = os.environ.get('GEMINI_API_KEY') or os.environ.get('GOOGLE_API_KEY')
    print(f"Testing connectivity with API Key: {api_key[:10]}...")
    
    try:
        client = genai.Client(api_key=api_key)
        # List models to verify key
        print("Models available:")
        for m in client.models.list():
            print(f"- {m.name}")
        print("Connectivity test successful!")
    except Exception as e:
        print(f"Connectivity test failed: {e}")

if __name__ == "__main__":
    test_connectivity()
