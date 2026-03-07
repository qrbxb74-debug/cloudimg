import os
import sys
import json
from datetime import datetime
import vertexai
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError
from vertexai.preview.vision_models import ImageGenerationModel
from google.api_core import exceptions as api_core_exceptions
import logging

class VertexGenerator:
    def __init__(self, project_id, location, temp_folder):
        # Clean project_id in case of accidental quotes in env vars
        self.project_id = project_id.strip('"').strip("'") if project_id else None
        self.location = location
        self.temp_folder = temp_folder
        self.model = None
        self.model_name = os.environ.get("VERTEX_MODEL_NAME", "imagen-3.0-generate-001")

        # This relies on GOOGLE_APPLICATION_CREDENTIALS env var being set
        # Initialize Credentials
        credentials = None
        
        # 1. Try loading from JSON String (Environment Variable) - Useful for Render/Heroku
        # Check GCP_CREDENTIALS_JSON first (as configured in Render), fallback to GOOGLE_CREDENTIALS_JSON
        json_creds = os.environ.get("GCP_CREDENTIALS_JSON") or os.environ.get("GOOGLE_CREDENTIALS_JSON")
        
        if not json_creds:
            logging.warning("VertexGenerator: No JSON credentials found in GCP_CREDENTIALS_JSON or GOOGLE_CREDENTIALS_JSON env vars.")

        if json_creds:
            try:
                info = json.loads(json_creds)
                # Debug: Print Key ID to verify which key is being used (matches GCP Console)
                key_id = info.get("private_key_id", "Unknown")
                client_email = info.get("client_email", "Unknown")
                print(f"VertexGenerator: Loading Creds for {client_email} | Key ID: {key_id}")
                
                credentials = service_account.Credentials.from_service_account_info(info)
                
                # If project_id wasn't set via env var, try to get it from the credentials JSON
                if not self.project_id or self.project_id == "your-gcp-project-id":
                    self.project_id = info.get("project_id")
                    if self.project_id:
                        logging.info(f"VertexGenerator: Extracted project_id '{self.project_id}' from JSON credentials.")
                    else:
                        logging.warning("VertexGenerator: JSON credentials loaded, but 'project_id' field was missing or empty.")

                logging.info("VertexGenerator: Loaded credentials from JSON env var.")
            except Exception as e:
                logging.error(f"VertexGenerator: Failed to load JSON credentials: {e}")
                print(f"VertexGenerator Error: JSON parsing failed - {e}", file=sys.stderr)

        if not self.project_id or self.project_id == "your-gcp-project-id":
            logging.error("VertexGenerator: GCP_PROJECT_ID is not configured.")
            return

        try:
            if credentials:
                vertexai.init(project=self.project_id, location=self.location, credentials=credentials)
            else:
                # Fallback to GOOGLE_APPLICATION_CREDENTIALS file or Metadata server
                vertexai.init(project=self.project_id, location=self.location)
            
            self.model = ImageGenerationModel.from_pretrained(self.model_name)
            logging.info(f"Vertex AI ImageGen initialized successfully for project: {self.project_id}")
            print(f"Vertex AI Ready: {self.project_id} @ {self.location}")
        except DefaultCredentialsError:
            msg = ("\n❌ VERTEX CREDENTIALS MISSING ❌\n"
                   "Please check GCP_CREDENTIALS_JSON in Render Environment.\n"
                   "Ensure the Service Account has 'Vertex AI User' role in GCP IAM.\n")
            logging.critical(msg)
            print(msg, file=sys.stderr)
        except Exception as e:
            logging.error(f"Failed to initialize Vertex AI: {e}")
            print(f"❌ Vertex AI Init Failed: {e}", file=sys.stderr)
            # Common error hint
            if "403" in str(e) or "PermissionDenied" in str(e):
                print("HINT: Check if 'Vertex AI User' role is assigned to the Service Account in GCP IAM.", file=sys.stderr)
                print("HINT: Also ensure 'Service Account Token Creator' role if using impersonation.", file=sys.stderr)

    def generate_image(self, prompt, aspect_ratio="1:1", user_id="anon", reference_image_path=None):
        """
        Generates an image using Vertex AI (Imagen 3).
        Accepts reference_image_path for interface compatibility but logs a warning as it's T2I.
        """
        if not self.model:
            return {"success": False, "error": "Vertex AI client not initialized."}

        # Strict Analysis: Input Validation
        if not prompt or not isinstance(prompt, str):
             logging.error(f"VertexGenerator: Invalid prompt provided by user {user_id}.")
             return {"success": False, "error": "INVALID_PROMPT"}

        # Map aspect ratios to Imagen supported values
        ratio_map = {
            "1:1": "1:1",
            "16:9": "16:9",
            "9:16": "9:16",
            "4:5": "3:4", # Closest match for Social
            "3:4": "3:4",
            "4:3": "4:3"
        }
        imagen_ratio = ratio_map.get(aspect_ratio, "1:1")

        logging.info(f"Vertex generating for user {user_id} | Ratio: {imagen_ratio} (requested {aspect_ratio})")

        if reference_image_path:
            logging.warning(f"VertexGenerator: Reference image passed but ignored (Model is Text-to-Image): {reference_image_path}")

        try:
            images = self.model.generate_images(
                prompt=prompt,
                number_of_images=1,
                aspect_ratio=imagen_ratio,
                safety_filter_level="block_some",
                person_generation="allow_adult"
            )

            if not images:
                logging.warning("VertexGenerator: Image generation returned no images (likely safety block).")
                return {"success": False, "error": "PROMPT_BLOCKED_SAFETY", "critical_stop": True}

            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"vertex_{user_id}_{timestamp}.png"
            filepath = os.path.join(self.temp_folder, filename)

            images[0].save(location=filepath, include_generation_parameters=True)
            logging.info(f"Vertex image saved: {filename}")

            return {"success": True, "filename": filename}

        except api_core_exceptions.PermissionDenied as e:
            logging.error(f"Vertex API Permission Denied: {e}")
            return {"success": False, "error": "API_PERMISSION_DENIED"}
        except api_core_exceptions.ResourceExhausted as e:
            logging.warning(f"Vertex API Quota Exceeded: {e}")
            return {"success": False, "error": "API_QUOTA_EXCEEDED"}
        except api_core_exceptions.InvalidArgument as e:
            error_str = str(e).lower()
            if "safety" in error_str or "blocked" in error_str:
                logging.warning(f"Vertex prompt blocked by safety filters: {prompt}")
                return {"success": False, "error": "PROMPT_BLOCKED_SAFETY", "critical_stop": True}
            logging.error(f"Vertex API Invalid Argument: {e}")
            return {"success": False, "error": "GENERATOR_INVALID_ARGUMENT"}
        except Exception as e:
            logging.error(f"Vertex Unexpected Error: {e}")
            return {"success": False, "error": "UNEXPECTED_GENERATION_ERROR"}