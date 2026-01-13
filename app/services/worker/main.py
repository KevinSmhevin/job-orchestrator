"""
Job worker main loop.

Continuously polls for jobs, executes them, and handles results.
"""
import signal
import sys
import time
import logging
from typing import Optional

from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.services.leasing import LeasingService
from app.services.worker.executor import JobExecutor
from app.settings import settings
from app.models.job import Job

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
_shutdown_flag = False


def signal_handler(signum, frame):
    """Handle shutdown signals (SIGINT, SIGTERM)."""
    global _shutdown_flag
    signal_name = signal.Signals(signum).name
    
    print(f"\n🛑 Received {signal_name} signal")
    print("⏳ Finishing current job before shutdown...")
    
    _shutdown_flag = True
    logger.info(f"Shutdown signal received: {signal_name}")


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # kill command


class Worker:
    """
    Job worker that processes jobs from the queue.
    
    The worker continuously:
    1. Claims the next available job
    2. Executes the job's handler
    3. Marks the job as succeeded or failed
    4. Repeats until shutdown
    
    Example:
        worker = Worker(
            worker_id="worker-1",
            queues=["default", "emails"],
            poll_interval=5,
            lease_seconds=60,
        )
        worker.run()
    """
    
    def __init__(
        self,
        worker_id: str,
        queues: list[str],
        poll_interval: int = 5,
        lease_seconds: int = 60,
        verbose: bool = True,
    ):
        """
        Initialize worker.
        
        Args:
            worker_id: Unique identifier for this worker
            queues: List of queue names to process
            poll_interval: Seconds to wait between polls when no jobs available
            lease_seconds: How long to hold lease on jobs
            verbose: Enable verbose output
        """
        self.worker_id = worker_id
        self.queues = queues
        self.poll_interval = poll_interval
        self.verbose = verbose
        
        # Initialize services
        self.leasing_service = LeasingService(default_lease_seconds=lease_seconds)
        self.executor = JobExecutor(verbose=verbose)
        
        # Stats
        self.jobs_processed = 0
        self.jobs_succeeded = 0
        self.jobs_failed = 0
        
        logger.info(f"Worker initialized: {worker_id}")
    
    def run(self):
        """
        Main worker loop.
        
        Continuously polls for and processes jobs until shutdown signal received.
        """
        self._print_startup_banner()
        
        try:
            while not _shutdown_flag:
                try:
                    # Process one job
                    job_processed = self._process_one_job()
                    
                    # If no job was available, sleep before next poll
                    if not job_processed and not _shutdown_flag:
                        self._sleep_with_interrupt_check(self.poll_interval)
                
                except KeyboardInterrupt:
                    # Handled by signal handler
                    break
                
                except Exception as e:
                    logger.exception(f"Worker error: {e}")
                    if self.verbose:
                        print(f"❌ Worker error: {e}")
                        import traceback
                        traceback.print_exc()
                    
                    # Sleep before retrying
                    if not _shutdown_flag:
                        time.sleep(5)
        
        finally:
            self._print_shutdown_banner()
    
    def _process_one_job(self) -> bool:
        """
        Attempt to claim and process one job.
        
        Returns:
            True if a job was processed, False if no jobs available
        """
        # Claim next job
        job = self._claim_job()
        
        if job is None:
            return False
        
        # Execute job
        result = self.executor.execute(job)
        
        # Complete job (mark success/failure)
        self._complete_job(job, result.success, result.error_message)
        
        # Update stats
        self.jobs_processed += 1
        if result.success:
            self.jobs_succeeded += 1
        else:
            self.jobs_failed += 1
        
        return True
    
    def _claim_job(self) -> Optional[Job]:
        """
        Claim the next available job.
        
        Returns:
            Job if one was claimed, None if no jobs available
        """
        try:
            with SessionLocal.begin() as session:
                job = self.leasing_service.claim_next_job(
                    session,
                    worker_id=self.worker_id,
                    queues=self.queues,
                )
                
                if job is None:
                    return None
                
                # ✅ Access attributes while session is still open
                # This loads them into the object before detachment
                job_id = job.id
                handler = job.handler
                payload = job.payload
                
                logger.info(f"Claimed job {job_id}")
                if self.verbose:
                    print(f"\n📦 Claimed job {job_id} (handler: {handler})")
                
                # Make job available outside session by refreshing attributes
                session.refresh(job)
                session.expunge(job)  # Detach from session so it can be used outside
                
                return job
    
        except Exception as e:
            logger.exception(f"Failed to claim job: {e}")
            if self.verbose:
                print(f"❌ Failed to claim job: {e}")
            return None
    
    def _complete_job(self, job: Job, success: bool, error: Optional[str]):
        """
        Mark job as completed (succeeded or failed).
        
        Args:
            job: Job that was executed
            success: Whether execution succeeded
            error: Error message if failed
        """
        try:
            with SessionLocal.begin() as session:
                self.leasing_service.complete_job(
                    session,
                    job_id=job.id,
                    worker_id=self.worker_id,
                    success=success,
                    error=error,
                )
                
                status = "✅ succeeded" if success else "❌ failed"
                logger.info(f"Job {job.id} {status}")
        
        except Exception as e:
            logger.exception(f"Failed to complete job {job.id}: {e}")
            if self.verbose:
                print(f"❌ Failed to complete job {job.id}: {e}")
    
    def _sleep_with_interrupt_check(self, seconds: int):
        """
        Sleep for specified seconds, checking for shutdown signal.
        
        Args:
            seconds: Total seconds to sleep
        """
        # Sleep in 1-second intervals to check shutdown flag
        for _ in range(seconds):
            if _shutdown_flag:
                break
            time.sleep(1)
    
    def _print_startup_banner(self):
        """Print worker startup information."""
        if not self.verbose:
            return
        
        print("\n" + "=" * 70)
        print("🚀 JOB WORKER STARTING")
        print("=" * 70)
        print(f"Worker ID:       {self.worker_id}")
        print(f"Queues:          {', '.join(self.queues)}")
        print(f"Poll interval:   {self.poll_interval}s")
        print(f"Lease duration:  {self.leasing_service.default_lease_seconds}s")
        print(f"Environment:     {settings.environment}")
        print("=" * 70)
        print("\n👀 Watching for jobs...\n")
        
        logger.info(f"Worker started: {self.worker_id}")
    
    def _print_shutdown_banner(self):
        """Print worker shutdown information."""
        if not self.verbose:
            return
        
        print("\n" + "=" * 70)
        print("✋ JOB WORKER STOPPED")
        print("=" * 70)
        print(f"Worker ID:        {self.worker_id}")
        print(f"Jobs processed:   {self.jobs_processed}")
        print(f"Jobs succeeded:   {self.jobs_succeeded}")
        print(f"Jobs failed:      {self.jobs_failed}")
        
        if self.jobs_processed > 0:
            success_rate = (self.jobs_succeeded / self.jobs_processed) * 100
            print(f"Success rate:     {success_rate:.1f}%")
        
        print("=" * 70)
        print("\n👋 Goodbye!\n")
        
        logger.info(
            f"Worker stopped: {self.worker_id} "
            f"(processed={self.jobs_processed}, "
            f"succeeded={self.jobs_succeeded}, "
            f"failed={self.jobs_failed})"
        )


def main():
    """
    CLI entry point for the worker.
    
    Loads configuration from settings and starts the worker.
    
    Run:
        python -m app.services.worker.main
        
    Or with uv:
        uv run python -m app.services.worker.main
    """
    # Load configuration from settings
    worker_id = settings.worker_id
    queues = settings.worker_queues
    poll_interval = settings.worker_poll_interval
    lease_seconds = settings.worker_lease_seconds
    
    # Validate configuration
    if not queues:
        print("❌ Error: No queues configured in WORKER_QUEUES")
        logger.error("No queues configured")
        sys.exit(1)
    
    # Create worker
    worker = Worker(
        worker_id=worker_id,
        queues=queues,
        poll_interval=poll_interval,
        lease_seconds=lease_seconds,
        verbose=True,
    )
    
    # Run worker
    try:
        worker.run()
    except KeyboardInterrupt:
        # Handled by signal handler
        pass
    except Exception as e:
        logger.exception(f"Fatal worker error: {e}")
        print(f"\n💥 Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()