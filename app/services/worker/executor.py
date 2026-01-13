"""
Job executor.

Handles the execution of individual jobs by looking up and calling
the appropriate handler function.
"""
import traceback
import time
import logging
from typing import Optional, Tuple
from uuid import UUID

from app.models.job import Job
from app.handlers import registry

logger = logging.getLogger(__name__)


class ExecutionResult:
    """Result of a job execution."""
    
    def __init__(
        self,
        success: bool,
        error_message: Optional[str] = None,
        duration_seconds: Optional[float] = None,
    ):
        self.success = success
        self.error_message = error_message
        self.duration_seconds = duration_seconds
    
    def __repr__(self) -> str:
        status = "SUCCESS" if self.success else "FAILED"
        duration = f"{self.duration_seconds:.2f}s" if self.duration_seconds else "N/A"
        return f"<ExecutionResult {status} duration={duration}>"
    
    def __bool__(self) -> bool:
        """Allow using result in boolean context."""
        return self.success


class JobExecutor:
    """
    Executes job handlers.
    
    Responsibilities:
    - Look up handler function from registry
    - Execute handler with job payload
    - Catch and format exceptions
    - Track execution time
    - Return structured result
    
    Example:
        executor = JobExecutor()
        result = executor.execute(job)
        
        if result.success:
            print(f"Job completed in {result.duration_seconds}s")
        else:
            print(f"Job failed: {result.error_message}")
    """
    
    def __init__(self, verbose: bool = True):
        """
        Initialize executor.
        
        Args:
            verbose: If True, print detailed execution info. Defaults to True.
        """
        self.verbose = verbose
    
    def execute(self, job: Job) -> ExecutionResult:
        """
        Execute a job's handler.
        
        Args:
            job: Job to execute
        
        Returns:
            ExecutionResult with success status, error message, and duration
        
        Example:
            result = executor.execute(job)
            
            if result.success:
                print("Job succeeded!")
            else:
                print(f"Job failed: {result.error_message}")
        """
        job_id = str(job.id)
        handler_name = job.handler
        payload = job.payload
        
        # Log execution start
        self._log_start(job_id, handler_name, payload)
        
        # Validate handler exists
        if not registry.exists(handler_name):
            error = self._handler_not_found_error(handler_name)
            return ExecutionResult(success=False, error_message=error)
        
        # Execute handler with timing
        start_time = time.time()
        
        try:
            # Get handler function
            handler_func = registry.get(handler_name)
            
            # Execute handler
            self._log_executing(handler_name)
            handler_func(payload)
            
            # Success!
            duration = time.time() - start_time
            self._log_success(job_id, duration)
            
            return ExecutionResult(
                success=True,
                error_message=None,
                duration_seconds=duration,
            )
            
        except Exception as e:
            # Failure
            duration = time.time() - start_time
            error_message = self._format_exception(e)
            
            self._log_failure(job_id, error_message, duration)
            
            return ExecutionResult(
                success=False,
                error_message=error_message,
                duration_seconds=duration,
            )
    
    # ─────────────────────────────────────────────────────────────────
    # Error Handling
    # ─────────────────────────────────────────────────────────────────
    
    def _handler_not_found_error(self, handler_name: str) -> str:
        """Format handler not found error."""
        available = ", ".join(registry.list())
        error = (
            f"Handler '{handler_name}' not registered. "
            f"Available handlers: [{available}]"
        )
        
        if self.verbose:
            print(f"❌ {error}")
        
        logger.error(f"Handler not found: {handler_name}")
        
        return error
    
    def _format_exception(self, exception: Exception) -> str:
        """
        Format exception into error message.
        
        Args:
            exception: Exception that occurred
        
        Returns:
            Formatted error message
        """
        # Get exception type and message
        exc_type = type(exception).__name__
        exc_message = str(exception)
        
        # Format error message
        if exc_message:
            error_message = f"{exc_type}: {exc_message}"
        else:
            error_message = exc_type
        
        # Print full traceback if verbose
        if self.verbose:
            print("\n" + "─" * 60)
            print("Exception Details:")
            print("─" * 60)
            traceback.print_exc()
            print("─" * 60 + "\n")
        
        # Log to logger
        logger.exception(f"Job execution failed: {error_message}")
        
        return error_message
    
    # ─────────────────────────────────────────────────────────────────
    # Logging Helpers
    # ─────────────────────────────────────────────────────────────────
    
    def _log_start(self, job_id: str, handler_name: str, payload: dict):
        """Log job execution start."""
        if self.verbose:
            print(f"\n{'=' * 70}")
            print(f"🔧 EXECUTING JOB")
            print(f"{'=' * 70}")
            print(f"Job ID:   {job_id}")
            print(f"Handler:  {handler_name}")
            print(f"Payload:  {self._truncate_payload(payload)}")
            print(f"{'=' * 70}\n")
        
        logger.info(f"Executing job {job_id} with handler '{handler_name}'")
    
    def _log_executing(self, handler_name: str):
        """Log handler execution."""
        if self.verbose:
            print(f"▶️  Running handler '{handler_name}'...\n")
    
    def _log_success(self, job_id: str, duration: float):
        """Log successful execution."""
        if self.verbose:
            print(f"\n{'=' * 70}")
            print(f"✅ JOB COMPLETED SUCCESSFULLY")
            print(f"{'=' * 70}")
            print(f"Job ID:   {job_id}")
            print(f"Duration: {duration:.2f}s")
            print(f"{'=' * 70}\n")
        
        logger.info(f"Job {job_id} completed successfully in {duration:.2f}s")
    
    def _log_failure(self, job_id: str, error_message: str, duration: float):
        """Log failed execution."""
        if self.verbose:
            print(f"\n{'=' * 70}")
            print(f"❌ JOB FAILED")
            print(f"{'=' * 70}")
            print(f"Job ID:   {job_id}")
            print(f"Error:    {error_message}")
            print(f"Duration: {duration:.2f}s")
            print(f"{'=' * 70}\n")
        
        logger.error(f"Job {job_id} failed after {duration:.2f}s: {error_message}")
    
    def _truncate_payload(self, payload: dict, max_length: int = 200) -> str:
        """
        Truncate payload for logging.
        
        Args:
            payload: Job payload
            max_length: Maximum string length
        
        Returns:
            Truncated payload string
        """
        payload_str = str(payload)
        if len(payload_str) > max_length:
            return payload_str[:max_length] + "..."
        return payload_str


# ─────────────────────────────────────────────────────────────────
# Convenience function
# ─────────────────────────────────────────────────────────────────

def execute_job(job: Job, verbose: bool = True) -> ExecutionResult:
    """
    Execute a job (convenience function).
    
    Args:
        job: Job to execute
        verbose: Print detailed execution info
    
    Returns:
        ExecutionResult
    
    Example:
        from app.services.worker.executor import execute_job
        
        result = execute_job(job)
        if result.success:
            print("Success!")
    """
    executor = JobExecutor(verbose=verbose)
    return executor.execute(job)


# ─────────────────────────────────────────────────────────────────
# Testing/Debug
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    """
    Test the executor with a mock job.
    
    Run: python -m app.services.worker.executor
    """
    from app.models.job import Job, JobStatus
    from datetime import datetime, timezone
    import uuid
    
    print("🧪 Testing JobExecutor\n")
    
    # Create mock job
    mock_job = Job(
        id=uuid.uuid4(),
        handler="hello_world",
        queue="default",
        status=JobStatus.running,
        run_at=datetime.now(timezone.utc),
        priority=0,
        payload={"name": "Test"},
        max_attempts=3,
        attempts=0,
        timeout_secs=300,
    )
    
    # Execute
    executor = JobExecutor(verbose=True)
    result = executor.execute(mock_job)
    
    # Check result
    print(f"\n📊 Result: {result}")
    print(f"   Success: {result.success}")
    print(f"   Duration: {result.duration_seconds:.3f}s")
    print(f"   Error: {result.error_message}")
    
    # Test with non-existent handler
    print("\n" + "─" * 70)
    print("Testing with invalid handler...\n")
    
    mock_job.handler = "nonexistent_handler"
    result = executor.execute(mock_job)
    
    print(f"\n📊 Result: {result}")
    print(f"   Success: {result.success}")
    print(f"   Error: {result.error_message}")
    
    # Test with failing handler
    print("\n" + "─" * 70)
    print("Testing with failing handler...\n")
    
    mock_job.handler = "always_fail"
    mock_job.payload = {"message": "Test failure"}
    result = executor.execute(mock_job)
    
    print(f"\n📊 Result: {result}")
    print(f"   Success: {result.success}")
    print(f"   Error: {result.error_message}")
    
    print("\n Executor testing complete!")