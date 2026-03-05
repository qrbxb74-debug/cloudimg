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
    def __init__(self, visual_recognizer, temp_storage_path, rate_limit_seconds=4.0, vertex_generator=None):
        """
        Initializes the Queue Manager.
        
        :param visual_recognizer: Instance of VisualRecognizer to perform analysis.
        :param vertex_generator: Instance of VertexGenerator to perform image generation.
        :param temp_storage_path: Folder to store files while they wait in queue.
        :param rate_limit_seconds: Minimum delay between processing tasks to respect API limits.
        """
        self.visual_recognizer = visual_recognizer
        self.vertex_generator = vertex_generator
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
        # Prevent starting multiple threads if one is already alive
        if self.worker_thread and self.worker_thread.is_alive():
            return

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
            
    def is_alive(self):
        """Checks if the worker thread is currently active."""
        return self.worker_thread is not None and self.worker_thread.is_alive()

    def add_to_queue(self, task_type, user_id, **kwargs):
        """
        Adds a task to the processing queue.
        
        :param task_type: 'analyze' or 'generate'.
        :param user_id: The ID of the user uploading the file.
        :param kwargs: For 'analyze', pass 'source_file_path'. For 'generate', pass 'prompt' and 'aspect_ratio'.
        :return: task_id (str) or None if failed
        """
        try:
            timestamp = int(time.time() * 1000)
            task = {
                'id': f"{user_id}_{timestamp}",
                'user_id': user_id,
                'type': task_type,
                'status': 'queued',
                'attempts': 0
            }

            if task_type == 'analyze':
                source_file_path = kwargs.get('source_file_path')
                if not source_file_path or not os.path.exists(source_file_path):
                    raise FileNotFoundError("Source file for analysis not found.")
                
                # Generate a unique filename for the queue storage to prevent collisions
                filename = os.path.basename(source_file_path)
                safe_filename = f"{timestamp}_{user_id}_{filename}"
                dest_path = os.path.join(self.temp_storage_path, safe_filename)
                
                # Copy the file to our safe temp storage
                shutil.copy2(source_file_path, dest_path)

                task.update({
                    'file_path': dest_path,
                    'source_file_path': source_file_path,
                    'original_name': filename,
                })

            elif task_type == 'generate':
                if not self.vertex_generator:
                    raise ValueError("VertexGenerator not configured in QueueManager.")
                prompt = kwargs.get('prompt')
                if not prompt:
                    raise ValueError("Prompt is required for generate task.")
                
                task.update({
                    'prompt': prompt,
                    'aspect_ratio': kwargs.get('aspect_ratio', '1:1')
                })
            else:
                raise ValueError(f"Unknown task type: {task_type}")

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
        for task_id, task in self.tasks.items():
            if str(task['user_id']) == str(user_id) and os.path.exists(task['file_path']):
                active_tasks.append(task)
        return active_tasks

    def _process_queue(self):
        """Internal loop to process items one by one. Made more robust to prevent thread death."""
        while self.is_running:
            task = None
            try:
                # Get task, wait up to 1 second to check is_running flag periodically
                task = self.queue.get(timeout=1)
            except queue.Empty:
                # This is normal when the queue is idle. Continue to the next loop iteration.
                continue
            except BaseException:
                # A more serious error with the queue itself.
                logger.exception("CRITICAL: Unhandled exception in queue.get(). Worker thread is recovering.")
                time.sleep(5) # Avoid fast-spinning on a persistent queue error
                continue

            # If we got a task, process it within a robust error-handling block.
            try:
                start_time = time.time()
                
                # Log that we are starting to process this specific task.
                logger.info(f"Worker picked up task {task['id']}. Starting processing.")
                
                task_type = task.get('type', 'analyze') # Default to analyze for old tasks
                if task_type == 'generate':
                    self._handle_generation_processing(task)
                else:
                    self._handle_analysis_processing(task)
                
                # Enforce Rate Limit
                # Calculate how long the request took, and sleep the remainder of the interval
                elapsed_time = time.time() - start_time
                if elapsed_time < self.rate_limit_seconds:
                    sleep_duration = self.rate_limit_seconds - elapsed_time
                    logger.info(f"Rate limit enforcement: Sleeping for {sleep_duration:.2f}s")
                    time.sleep(sleep_duration)

            except BaseException as e:
                # This is the key change: catch *any* exception during processing to prevent thread death.
                logger.exception(f"Gemini worker recovered from an unhandled exception while processing task {task.get('id', 'UNKNOWN')}.")
                
                # Ensure the task is marked as failed if it wasn't already.
                if task and task.get('id') in self.tasks:
                    task_ref = self.tasks[task['id']]
                    if task_ref.get('status') != 'failed':
                        task_ref['status'] = 'failed'
                task_ref['error'] = 'WORKER_CRASH'
                
                time.sleep(1) # Small delay before picking up the next task.
            finally:
                # This is crucial. It signals that the task is finished, allowing the queue to proceed.
                # It must be called regardless of success or failure.
                if task:
                    self.queue.task_done()

    def _handle_analysis_processing(self, task):
        """
        The logic that calls the Gemini Vision API for analysis.
        """
        file_path = task['file_path']
        source_path = task.get('source_file_path')
        
        # This try-except is for handling specific logic within the task,
        # while the one in _process_queue is a safety net for the whole thread.
        try:
            if not os.path.exists(file_path):
                logger.warning(f"File for task {task['id']} not found at path: {file_path}. Marking as failed.")
                task['status'] = 'failed'
                task['error'] = 'FILE_NOT_FOUND_IN_STORAGE'
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
                task['error'] = result.get('error', 'UNKNOWN_ANALYSIS_ERROR')
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
            task['error'] = 'UNEXPECTED_ANALYSIS_ERROR'
            logger.exception(f"Critical error in _handle_gemini_processing for task {task['id']}")
            
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
                        logger.info(f"Cleaned up temp queue file for failed task {task['id']}: {file_path}")
                except OSError as e:
                    logger.error(f"Error cleaning up temp queue file {file_path}: {e}")

    def _handle_generation_processing(self, task):
        """
        The logic that calls the Vertex AI API for image generation.
        """
        try:
            task['status'] = 'processing'
            
            # Call the actual AI generation
            result = self.vertex_generator.generate_image(
                prompt=task['prompt'],
                aspect_ratio=task['aspect_ratio'],
                user_id=task['user_id']
            )
            
            if result['success'] and result.get('filename'):
                task['status'] = 'completed'
                # The result is the filename of the newly created image in the temp folder
                task['result'] = {'generated_filename': result['filename']}
                logger.info(f"Generation Task {task['id']} completed successfully.")
            else:
                task['status'] = 'failed'
                task['error'] = result.get('error', 'UNKNOWN_GENERATION_ERROR')
                task['critical_stop'] = result.get('critical_stop', False)
                logger.warning(f"Generation Task {task['id']} failed: {task['error']}")
        except Exception as e:
            task['status'] = 'failed'
            task['error'] = 'UNEXPECTED_GENERATION_ERROR'
            logger.exception(f"Critical error in _handle_generation_processing for task {task['id']}")