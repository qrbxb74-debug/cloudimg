import os
import json
import base64
import traceback
from datetime import datetime
import vertexai
from google.auth.exceptions import DefaultCredentialsError
from vertexai.preview.vision_models import ImageGenerationModel
from google.api_core import exceptions as api_core_exceptions
import logging

class VertexGenerator:
    def __init__(self, project_id, location, temp_folder):
        self.project_id = project_id
        self.location = location
        self.temp_folder = temp_folder
        self.model = None
        self.model_name = os.environ.get("VERTEX_MODEL_NAME", "imagen-3.0-generate-001")
        
        print(f"DEBUG: VertexGenerator initializing. Input project_id: {self.project_id}")

        # --- Fix for Render: Load Credentials from Env Var ---
        # Render doesn't support file uploads for secrets easily, so we use an Env Var.
        if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            creds_json = os.environ.get("GCP_CREDENTIALS_JSON")
            if creds_json:
                print(f"DEBUG: Found GCP_CREDENTIALS_JSON (Length: {len(creds_json)})")
                try:
                    # Handle both raw JSON and Base64 encoded JSON
                    try:
                        creds_data = json.loads(creds_json)
                    except json.JSONDecodeError:
                        creds_data = json.loads(base64.b64decode(creds_json).decode('utf-8'))
                    
                    # Auto-detect project_id if missing
                    if not self.project_id and 'project_id' in creds_data:
                        self.project_id = creds_data['project_id']
                        print(f"DEBUG: Auto-detected project_id from credentials: {self.project_id}")

                    creds_path = os.path.join(self.temp_folder, "gcp_credentials.json")
                    with open(creds_path, "w") as f:
                        json.dump(creds_data, f)
                    
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
                    print(f"DEBUG: Credentials loaded from GCP_CREDENTIALS_JSON to {creds_path}")
                except Exception as e:
                    print(f"ERROR: VertexGenerator: Failed to process GCP_CREDENTIALS_JSON: {e}")
                    traceback.print_exc()
            else:
                print("DEBUG: GCP_CREDENTIALS_JSON not found in environment.")

        if not self.project_id or self.project_id == "your-gcp-project-id":
            print("ERROR: VertexGenerator: GCP_PROJECT_ID is not configured and could not be extracted from credentials.")
            return

        # This relies on GOOGLE_APPLICATION_CREDENTIALS env var being set
        try:
            vertexai.init(project=self.project_id, location=self.location)
            self.model = ImageGenerationModel.from_pretrained(self.model_name)
            print(f"SUCCESS: Vertex AI ImageGen initialized successfully for project: {self.project_id}")
        except DefaultCredentialsError:
            logging.critical("\n\n❌ VERTEX CREDENTIALS MISSING ❌\n"
                             "You must set up Application Default Credentials for the server.\n"
                             "1. Create a Service Account in your GCP project.\n"
                             "2. Grant it the 'Vertex AI User' role.\n"
                             "3. Download the JSON key for the service account.\n"
                             "4. Set the environment variable 'GOOGLE_APPLICATION_CREDENTIALS' to the path of that JSON file.\n"
                             "The app will not be ableto generate images until this is done.\n\n")
        except Exception as e:
            print(f"ERROR: Failed to initialize Vertex AI: {e}")
            traceback.print_exc()

    def generate_image(self, prompt, aspect_ratio="1:1", user_id="anon"):
        if not self.model:
            return {"success": False, "error": "Vertex AI client not initialized."}

        logging.info(f"Generating image for prompt: '{prompt}' with aspect ratio {aspect_ratio}")
        try:
            images = self.model.generate_images(
                prompt=prompt,
                number_of_images=1,
                aspect_ratio=aspect_ratio,
                safety_filter_level="block_some",
                person_generation="allow_adult"
            )

            if not images:
                logging.warning("Image generation returned no images. Prompt may have been blocked.")
                return {"success": False, "error": "PROMPT_BLOCKED_SAFETY", "critical_stop": True}

            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"generated_{user_id}_{timestamp}.png"
            filepath = os.path.join(self.temp_folder, filename)

            images[0].save(location=filepath, include_generation_parameters=True)
            logging.info(f"Successfully generated and saved image to {filepath}")

            return {"success": True, "filename": filename}

        except api_core_exceptions.PermissionDenied as e:
            logging.error(f"Vertex API Permission Denied: {e}. Ensure the service account has the 'Vertex AI User' role.")
            return {"success": False, "error": "API_PERMISSION_DENIED"}
        except api_core_exceptions.ResourceExhausted as e:
            logging.warning(f"Vertex API Quota Exceeded: {e}")
            return {"success": False, "error": "API_QUOTA_EXCEEDED"}
        except api_core_exceptions.InvalidArgument as e:
            if "safety policy" in str(e).lower() or "blocked" in str(e).lower():
                logging.warning(f"Image generation prompt was blocked by safety filters: {prompt}")
                return {"success": False, "error": "PROMPT_BLOCKED_SAFETY", "critical_stop": True}
            logging.error(f"Vertex API Invalid Argument: {e}")
            return {"success": False, "error": "GENERATOR_INVALID_ARGUMENT"}
        except Exception as e:
            logging.error(f"An unexpected error occurred during image generation: {e}")
            return {"success": False, "error": "UNEXPECTED_GENERATION_ERROR"}
