import threading
import queue
import time
import os
import shutil
import logging
from datetime import datetime

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("GeminiQueue")

class UploadQueueManager:
    def __init__(self, visual_recognizer, temp_storage_path='temp_queue_storage', rate_limit_seconds=4.0):
        """
        Initializes the Queue Manager.
        
        :param visual_recognizer: Instance of VisualRecognizer to perform analysis.
        :param temp_storage_path: Folder to store files while they wait in queue.
        :param rate_limit_seconds: Minimum delay between processing tasks to respect API limits.
        """
        self.visual_recognizer = visual_recognizer
        self.queue = queue.Queue()
        self.temp_storage_path = temp_storage_path
        self.rate_limit_seconds = rate_limit_seconds
        self.is_running = False
        self.worker_thread = None
        self.tasks = {} # Store task states: {id: task_dict}
        
        # Create temp directory if it doesn't exist
        if not os.path.exists(self.temp_storage_path):
            os.makedirs(self.temp_storage_path)
            
    def start_worker(self):
        """Starts the background processing thread."""
        if not self.is_running:
            self.is_running = True
            self.worker_thread = threading.Thread(target=self._process_queue, daemon=True)
            self.worker_thread.start()
            logger.info("Queue worker started.")

    def stop_worker(self):
        """Stops the background processing thread."""
        self.is_running = False
        if self.worker_thread:
            self.worker_thread.join()
            logger.info("Queue worker stopped.")

    def add_to_queue(self, source_file_path, user_id, request_data=None):
        """
        Adds a file upload to the processing queue.
        
        :param source_file_path: The current location of the uploaded file.
        :param user_id: The ID of the user uploading the file.
        :param request_data: Any extra data needed for the API call (e.g., prompts).
        :return: task_id (str) or None if failed
        """
        try:
            # Generate a unique filename for the queue storage to prevent collisions
            filename = os.path.basename(source_file_path)
            timestamp = int(time.time() * 1000)
            safe_filename = f"{timestamp}_{user_id}_{filename}"
            dest_path = os.path.join(self.temp_storage_path, safe_filename)
            
            # Move or Copy the file to our safe temp storage
            # We use copy2 to preserve metadata, assuming the web server might clean up the original source
            shutil.copy2(source_file_path, dest_path)
            
            task = {
                'id': f"{user_id}_{timestamp}",
                'file_path': dest_path,
                'source_file_path': source_file_path,
                'original_name': filename,
                'user_id': user_id,
                'data': request_data or {},
                'status': 'pending',
                'attempts': 0
            }
            
            self.tasks[task['id']] = task
            self.queue.put(task)
            logger.info(f"Task {task['id']} added to queue. Queue Size: {self.queue.qsize()}")
            return task['id']
            
        except Exception as e:
            logger.error(f"Failed to add file to queue: {str(e)}")
            return None

    def get_task_status(self, task_id):
        """Returns the current status dictionary of a task."""
        return self.tasks.get(task_id)

    def get_tasks_for_user(self, user_id):
        """Returns active tasks for a specific user where the file still exists."""
        active_tasks = []
        for task in self.tasks.values():
            if str(task['user_id']) == str(user_id) and os.path.exists(task['file_path']):
                active_tasks.append(task)
        return active_tasks

    def _process_queue(self):
        """Internal loop to process items one by one."""
        while self.is_running:
            try:
                # Get task, wait up to 1 second to check is_running flag periodically
                task = self.queue.get(timeout=1)
            except queue.Empty:
                continue

            start_time = time.time()
            
            try:
                self._handle_gemini_processing(task)
            except Exception as e:
                logger.error(f"Error processing task {task['id']}: {str(e)}")
                # Future: Add retry logic here if needed
            finally:
                self.queue.task_done()

            # Enforce Rate Limit
            # Calculate how long the request took, and sleep the remainder of the interval
            elapsed_time = time.time() - start_time
            if elapsed_time < self.rate_limit_seconds:
                sleep_duration = self.rate_limit_seconds - elapsed_time
                logger.info(f"Rate limit enforcement: Sleeping for {sleep_duration:.2f}s")
                time.sleep(sleep_duration)

    def _handle_gemini_processing(self, task):
        """
        The logic that actually calls the Gemini API.
        """
        logger.info(f"Processing task {task['id']} for user {task['user_id']}...")
        
        file_path = task['file_path']
        source_path = task.get('source_file_path')
        
        try:
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                task['status'] = 'failed'
                task['error'] = 'File not found in queue'
                return

            task['status'] = 'processing'
            
            # Call the actual AI analysis
            result = self.visual_recognizer.analyze_image(file_path)
            
            if result['success'] and result.get('data'):
                task['status'] = 'completed'
                task['result'] = result['data']
                logger.info(f"Task {task['id']} completed successfully.")
            else:
                task['status'] = 'failed'
                task['error'] = result.get('error')
                task['critical_stop'] = result.get('critical_stop', False)
                logger.warning(f"Task {task['id']} failed: {task['error']}")
                
                # Delete the original file in TEMP_FOLDER to prevent publishing
                if source_path and os.path.exists(source_path):
                    try:
                        os.remove(source_path)
                        logger.info(f"Deleted original file due to failure: {source_path}")
                    except Exception as e:
                        logger.error(f"Failed to delete original file {source_path}: {e}")

        except Exception as e:
            task['status'] = 'failed'
            task['error'] = f"Unexpected error: {str(e)}"
            logger.error(f"Critical error in task {task['id']}: {e}")
            
            # Emergency cleanup of source file
            if source_path and os.path.exists(source_path):
                try:
                    os.remove(source_path)
                except: pass

        finally:
            # Cleanup: Only remove the temp file if it FAILED. 
            # If success, we keep it so the user can publish it later.
            if task.get('status') == 'failed':
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except OSError:
                    pass