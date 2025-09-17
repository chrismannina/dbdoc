"""Job management system for tracking long-running operations."""

import logging
import uuid
import time
import asyncio
from typing import Dict, Any, Optional, Callable, List
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, Future

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Job execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobType(Enum):
    """Types of jobs that can be executed."""
    SCHEMA_DISCOVERY = "schema_discovery"
    DESCRIPTION_GENERATION = "description_generation"
    RELATIONSHIP_DETECTION = "relationship_detection"
    ERD_GENERATION = "erd_generation"
    DATA_PROFILING = "data_profiling"


@dataclass
class JobProgress:
    """Progress information for a job."""
    current_step: str
    steps_completed: int
    total_steps: int
    items_processed: int
    total_items: int
    percentage: float = 0.0
    estimated_remaining_seconds: Optional[float] = None
    
    def __post_init__(self):
        """Calculate percentage after initialization."""
        if self.total_items > 0:
            self.percentage = (self.items_processed / self.total_items) * 100
        elif self.total_steps > 0:
            self.percentage = (self.steps_completed / self.total_steps) * 100


@dataclass
class Job:
    """Represents a long-running job."""
    id: str
    job_type: JobType
    title: str
    description: str
    status: JobStatus = JobStatus.PENDING
    progress: JobProgress = field(default_factory=lambda: JobProgress("Initializing", 0, 1, 0, 1))
    result: Optional[Any] = None
    error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_by: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate job duration in seconds."""
        if self.started_at:
            end_time = self.completed_at or datetime.utcnow()
            return (end_time - self.started_at).total_seconds()
        return None


class JobManager:
    """Manages long-running jobs with progress tracking."""
    
    def __init__(self, max_concurrent_jobs: int = 5):
        """Initialize job manager."""
        self.jobs: Dict[str, Job] = {}
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_jobs)
        self.running_futures: Dict[str, Future] = {}
        self.progress_callbacks: Dict[str, List[Callable[[Job], None]]] = {}
        self._lock = threading.Lock()
        
    def create_job(self, 
                   job_type: JobType,
                   title: str,
                   description: str,
                   total_items: int = 1,
                   metadata: Optional[Dict[str, Any]] = None,
                   created_by: Optional[str] = None) -> str:
        """Create a new job and return its ID."""
        job_id = str(uuid.uuid4())
        
        job = Job(
            id=job_id,
            job_type=job_type,
            title=title,
            description=description,
            progress=JobProgress("Queued", 0, 1, 0, total_items),
            metadata=metadata or {},
            created_by=created_by
        )
        
        with self._lock:
            self.jobs[job_id] = job
            self.progress_callbacks[job_id] = []
        
        logger.info(f"Created job {job_id}: {title}")
        return job_id
    
    def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID."""
        return self.jobs.get(job_id)
    
    def list_jobs(self, 
                  job_type: Optional[JobType] = None,
                  status: Optional[JobStatus] = None,
                  limit: int = 50) -> List[Job]:
        """List jobs with optional filtering."""
        jobs = list(self.jobs.values())
        
        if job_type:
            jobs = [j for j in jobs if j.job_type == job_type]
        
        if status:
            jobs = [j for j in jobs if j.status == status]
        
        # Sort by creation time, newest first
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        
        return jobs[:limit]
    
    def start_job(self, 
                  job_id: str,
                  job_function: Callable,
                  *args,
                  **kwargs) -> bool:
        """Start executing a job."""
        job = self.jobs.get(job_id)
        if not job:
            logger.error(f"Job {job_id} not found")
            return False
        
        if job.status != JobStatus.PENDING:
            logger.error(f"Job {job_id} is not in pending status")
            return False
        
        # Create progress callback
        def progress_callback(current_step: str, steps_completed: int, total_steps: int,
                            items_processed: int, total_items: int):
            self.update_job_progress(job_id, current_step, steps_completed, total_steps,
                                   items_processed, total_items)
        
        # Submit job to executor
        def job_wrapper():
            try:
                # Update status to running
                self._update_job_status(job_id, JobStatus.RUNNING)
                job.started_at = datetime.utcnow()
                
                # Execute the job function with progress callback
                result = job_function(progress_callback, *args, **kwargs)
                
                # Update job with result
                with self._lock:
                    job.result = result
                    job.status = JobStatus.COMPLETED
                    job.completed_at = datetime.utcnow()
                    job.progress.current_step = "Completed"
                    job.progress.percentage = 100.0
                
                self._notify_progress_callbacks(job_id)
                logger.info(f"Job {job_id} completed successfully")
                
                return result
                
            except Exception as e:
                # Update job with error
                with self._lock:
                    job.error = str(e)
                    job.status = JobStatus.FAILED
                    job.completed_at = datetime.utcnow()
                    job.progress.current_step = f"Failed: {str(e)}"
                
                self._notify_progress_callbacks(job_id)
                logger.error(f"Job {job_id} failed: {e}")
                raise
        
        # Submit to thread pool
        future = self.executor.submit(job_wrapper)
        
        with self._lock:
            self.running_futures[job_id] = future
        
        logger.info(f"Started job {job_id}")
        return True
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job."""
        job = self.jobs.get(job_id)
        if not job:
            return False
        
        if job.status not in [JobStatus.PENDING, JobStatus.RUNNING]:
            return False
        
        # Cancel the future if it's running
        future = self.running_futures.get(job_id)
        if future:
            cancelled = future.cancel()
            if cancelled or future.done():
                with self._lock:
                    job.status = JobStatus.CANCELLED
                    job.completed_at = datetime.utcnow()
                    job.progress.current_step = "Cancelled"
                
                self._notify_progress_callbacks(job_id)
                logger.info(f"Cancelled job {job_id}")
                return True
        
        return False
    
    def update_job_progress(self,
                           job_id: str,
                           current_step: str,
                           steps_completed: int,
                           total_steps: int,
                           items_processed: int,
                           total_items: int):
        """Update job progress."""
        job = self.jobs.get(job_id)
        if not job:
            return
        
        with self._lock:
            job.progress = JobProgress(
                current_step=current_step,
                steps_completed=steps_completed,
                total_steps=total_steps,
                items_processed=items_processed,
                total_items=total_items
            )
            
            # Estimate remaining time
            if job.started_at and items_processed > 0:
                elapsed = (datetime.utcnow() - job.started_at).total_seconds()
                rate = items_processed / elapsed
                remaining_items = total_items - items_processed
                job.progress.estimated_remaining_seconds = remaining_items / rate if rate > 0 else None
        
        self._notify_progress_callbacks(job_id)
    
    def add_progress_callback(self, job_id: str, callback: Callable[[Job], None]):
        """Add a progress callback for a job."""
        with self._lock:
            if job_id in self.progress_callbacks:
                self.progress_callbacks[job_id].append(callback)
    
    def remove_progress_callback(self, job_id: str, callback: Callable[[Job], None]):
        """Remove a progress callback for a job."""
        with self._lock:
            if job_id in self.progress_callbacks:
                try:
                    self.progress_callbacks[job_id].remove(callback)
                except ValueError:
                    pass
    
    def cleanup_completed_jobs(self, max_age_hours: int = 24):
        """Clean up old completed jobs."""
        cutoff = datetime.utcnow().timestamp() - (max_age_hours * 3600)
        
        to_remove = []
        for job_id, job in self.jobs.items():
            if (job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED] and
                job.completed_at and job.completed_at.timestamp() < cutoff):
                to_remove.append(job_id)
        
        with self._lock:
            for job_id in to_remove:
                del self.jobs[job_id]
                if job_id in self.progress_callbacks:
                    del self.progress_callbacks[job_id]
                if job_id in self.running_futures:
                    del self.running_futures[job_id]
        
        if to_remove:
            logger.info(f"Cleaned up {len(to_remove)} old jobs")
    
    def _update_job_status(self, job_id: str, status: JobStatus):
        """Update job status."""
        with self._lock:
            job = self.jobs.get(job_id)
            if job:
                job.status = status
    
    def _notify_progress_callbacks(self, job_id: str):
        """Notify all progress callbacks for a job."""
        job = self.jobs.get(job_id)
        callbacks = self.progress_callbacks.get(job_id, [])
        
        for callback in callbacks:
            try:
                callback(job)
            except Exception as e:
                logger.error(f"Error in progress callback: {e}")
    
    def shutdown(self):
        """Shutdown the job manager."""
        logger.info("Shutting down job manager...")
        
        # Cancel all running jobs
        for job_id in list(self.running_futures.keys()):
            self.cancel_job(job_id)
        
        # Shutdown executor
        self.executor.shutdown(wait=True)
        logger.info("Job manager shutdown complete")


# Global job manager instance
job_manager = JobManager()


def create_job(job_type: JobType, title: str, description: str, 
               total_items: int = 1, metadata: Optional[Dict[str, Any]] = None,
               created_by: Optional[str] = None) -> str:
    """Convenience function to create a job."""
    return job_manager.create_job(job_type, title, description, total_items, metadata, created_by)


def start_job(job_id: str, job_function: Callable, *args, **kwargs) -> bool:
    """Convenience function to start a job."""
    return job_manager.start_job(job_id, job_function, *args, **kwargs)


def get_job(job_id: str) -> Optional[Job]:
    """Convenience function to get a job."""
    return job_manager.get_job(job_id)


def list_jobs(**kwargs) -> List[Job]:
    """Convenience function to list jobs."""
    return job_manager.list_jobs(**kwargs)


def cancel_job(job_id: str) -> bool:
    """Convenience function to cancel a job."""
    return job_manager.cancel_job(job_id)