import threading
import uuid
import time
from datetime import datetime
from typing import Dict, Any, List, Optional

class JobManager:
    def __init__(self):
        # Dictionary to store job details
        # Structure: { job_id: { "thread": Thread, "status": str, "progress": float, "logs": [], "result": Any, "error": str } }
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def start_job(self, target_func, args=(), name="Task") -> str:
        """
        Starts a background job.
        :param target_func: The function to run. It must accept 'job_id' and 'manager' as its first two arguments.
        :param args: Tuple of arguments for the task function.
        :param name: Name of the task.
        :return: The job ID.
        """
        job_id = str(uuid.uuid4())

        with self._lock:
            self.jobs[job_id] = {
                "id": job_id,
                "name": name,
                "status": "running",
                "progress": 0.0,
                "logs": [],
                "result": None,
                "error": None,
                "start_time": datetime.now()
            }

        def job_wrapper():
            try:
                # --- FIX: Pass (job_id, self, *args) ---
                # This matches: run_scraping_task(job_id, manager, states, api_key)
                result = target_func(job_id, self, *args)

                with self._lock:
                    if job_id in self.jobs:
                        self.jobs[job_id]["result"] = result
                        self.jobs[job_id]["status"] = "completed"
                        self.jobs[job_id]["progress"] = 1.0
                self.add_log(job_id, "✅ Task completed successfully.")
            except Exception as e:
                with self._lock:
                    if job_id in self.jobs:
                        self.jobs[job_id]["status"] = "failed"
                        self.jobs[job_id]["error"] = str(e)
                        self.jobs[job_id]["logs"].append(f"❌ Critical Error: {str(e)}")
                print(f"Job {job_id} failed: {e}")

        thread = threading.Thread(target=job_wrapper, daemon=True)
        with self._lock:
            if job_id in self.jobs:
                self.jobs[job_id]["thread"] = thread
        thread.start()
        return job_id

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self.jobs.get(job_id)

    def get_active_jobs(self) -> List[Dict[str, Any]]:
        """Returns a list of all jobs (running, failed, or completed) so UI can show logs."""
        with self._lock:
            # Sort jobs so the most recent ones appear first in the sidebar
            sorted_jobs = sorted(
                self.jobs.values(),
                key=lambda x: x['start_time'],
                reverse=True
            )

            # Return up to the 5 most recent jobs
            return [
                {"id": info["id"], **info}
                for info in sorted_jobs[:5]
            ]

    def update_progress(self, job_id: str, progress: float, message: str = None):
        """Updates the progress and optionally adds a log message."""
        with self._lock:
            if job_id in self.jobs:
                self.jobs[job_id]["progress"] = max(0.0, min(1.0, progress))
                if message:
                    timestamp = datetime.now().strftime('%H:%M:%S')
                    self.jobs[job_id]["logs"].append(f"[{timestamp}] {message}")

    def add_log(self, job_id: str, message: str):
        """Adds a log message to the job."""
        with self._lock:
            if job_id in self.jobs:
                timestamp = datetime.now().strftime('%H:%M:%S')
                self.jobs[job_id]["logs"].append(f"[{timestamp}] {message}")
