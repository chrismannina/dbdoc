"""Async generation engine for concurrent AI description generation with smart dependencies."""

import asyncio
import logging
import time
from typing import List, Dict, Any, Optional, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
from sqlalchemy.orm import Session
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json

from ..models import Table, Column, AIDescription, ValidationStatus
from ..services.ai_service import AIService, LLMProvider
from .enhanced_context_builder import EnhancedContextBuilder, EnhancedContext

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class GenerationTask:
    """Represents a single description generation task."""
    id: str
    target_type: str  # 'table' or 'column'
    target_id: int
    target_name: str
    priority: int = 0
    dependencies: Set[str] = field(default_factory=set)
    status: TaskStatus = TaskStatus.PENDING
    context: Optional[EnhancedContext] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    attempts: int = 0
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class GenerationProgress:
    """Progress information for generation process."""
    total_tasks: int
    completed: int
    failed: int
    running: int
    pending: int
    estimated_remaining_time: Optional[float] = None
    current_task: Optional[str] = None


class AsyncGenerationEngine:
    """Async engine for generating AI descriptions with concurrency and dependency management."""
    
    def __init__(
        self,
        db_session: Session,
        ai_service: AIService,
        context_builder: EnhancedContextBuilder,
        max_concurrent: int = 5,
        rate_limit_rpm: int = 60,
        max_retries: int = 3,
        cache_enabled: bool = True
    ):
        self.db = db_session
        self.ai_service = ai_service
        self.context_builder = context_builder
        self.max_concurrent = max_concurrent
        self.rate_limit_rpm = rate_limit_rpm
        self.max_retries = max_retries
        self.cache_enabled = cache_enabled
        
        # Rate limiting
        self.request_times = []
        self.rate_limit_semaphore = asyncio.Semaphore(max_concurrent)
        
        # Task management
        self.tasks: Dict[str, GenerationTask] = {}
        self.task_queue = asyncio.Queue()
        self.completed_tasks = []
        self.failed_tasks = []
        
        # Progress tracking
        self.progress_callback: Optional[Callable[[GenerationProgress], None]] = None
        self.start_time: Optional[float] = None
        
        # Context cache
        self.context_cache: Dict[str, str] = {}  # hash -> cached context
        self.result_cache: Dict[str, Dict[str, Any]] = {}  # context_hash -> result
        
    def set_progress_callback(self, callback: Callable[[GenerationProgress], None]):
        """Set callback for progress updates."""
        self.progress_callback = callback
    
    def _create_task_id(self, target_type: str, target_id: int) -> str:
        """Create unique task ID."""
        return f"{target_type}_{target_id}"
    
    def _calculate_priority(self, target_type: str, table: Table, column: Optional[Column] = None) -> int:
        """Calculate task priority (higher = more important)."""
        priority = 0
        
        # Tables get higher priority than columns
        if target_type == 'table':
            priority += 100
        
        # Larger tables get higher priority
        if table.row_count:
            priority += min(table.row_count // 1000, 50)
        
        # Key columns get higher priority  
        if column and column.is_key:
            priority += 25
            
        # PII columns get higher priority
        if column and column.is_pii:
            priority += 15
            
        return priority
    
    def _build_dependency_graph(self, tables: List[Table], selected_column_ids: Optional[Set[int]] = None) -> Dict[str, Set[str]]:
        """Build dependency graph where tables must be generated before their columns."""
        dependencies = {}
        
        for table in tables:
            table_task_id = self._create_task_id('table', table.id)
            dependencies[table_task_id] = set()  # Tables have no dependencies
            
            # Add column tasks that depend on table task
            for column in table.columns:
                if selected_column_ids is None or column.id in selected_column_ids:
                    column_task_id = self._create_task_id('column', column.id)
                    dependencies[column_task_id] = {table_task_id}
                    
        return dependencies
    
    def _get_context_hash(self, context: EnhancedContext) -> str:
        """Generate hash of context for caching."""
        # Create a simplified context for hashing
        hash_data = {
            'target_name': context.target_name,
            'target_type': context.target_type,
            'basic_metadata': context.basic_metadata,
            'relationships': [
                {'source': r.source_table, 'target': r.target_table}
                for r in context.relationships
            ],
            'domain_hints': sorted(context.domain_hints),
            'schema_patterns': {
                'business_domains': sorted(context.schema_patterns.business_domains),
                'naming_convention': context.schema_patterns.naming_convention
            }
        }
        
        hash_str = json.dumps(hash_data, sort_keys=True)
        return hashlib.md5(hash_str.encode()).hexdigest()
    
    async def _rate_limit(self):
        """Enforce rate limiting."""
        now = time.time()
        
        # Remove old request times (older than 1 minute)
        self.request_times = [t for t in self.request_times if now - t < 60]
        
        # Check if we're under the rate limit
        if len(self.request_times) >= self.rate_limit_rpm:
            # Calculate how long to wait
            oldest_request = min(self.request_times)
            wait_time = 60 - (now - oldest_request)
            if wait_time > 0:
                logger.info(f"Rate limit reached, waiting {wait_time:.1f} seconds")
                await asyncio.sleep(wait_time)
        
        self.request_times.append(now)
    
    async def _generate_description(self, task: GenerationTask) -> Dict[str, Any]:
        """Generate a single description with caching and rate limiting."""
        # Check cache first
        if self.cache_enabled and task.context:
            context_hash = self._get_context_hash(task.context)
            if context_hash in self.result_cache:
                logger.info(f"Using cached result for {task.target_name}")
                return self.result_cache[context_hash]
        
        # Apply rate limiting
        await self._rate_limit()
        
        # Generate description
        try:
            if task.target_type == 'table':
                result = await self._generate_table_description(task)
            else:
                result = await self._generate_column_description(task)
                
            # Cache result
            if self.cache_enabled and task.context:
                context_hash = self._get_context_hash(task.context)
                self.result_cache[context_hash] = result
                
            return result
            
        except Exception as e:
            logger.error(f"Failed to generate description for {task.target_name}: {e}")
            raise
    
    async def _generate_table_description(self, task: GenerationTask) -> Dict[str, Any]:
        """Generate table description using enhanced context."""
        if not task.context:
            raise ValueError("Task context is required")
        
        # Build enhanced prompt with rich context
        context_str = self._build_table_context_string(task.context)
        
        # Run AI generation in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            result = await loop.run_in_executor(
                executor,
                self._sync_generate_table_description,
                context_str,
                task.context.basic_metadata
            )
            
        return result
    
    async def _generate_column_description(self, task: GenerationTask) -> Dict[str, Any]:
        """Generate column description using enhanced context."""
        if not task.context:
            raise ValueError("Task context is required")
            
        # Build enhanced prompt with rich context
        context_str = self._build_column_context_string(task.context)
        
        # Run AI generation in thread pool
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            result = await loop.run_in_executor(
                executor,
                self._sync_generate_column_description,
                context_str,
                task.context.basic_metadata
            )
            
        return result
    
    def _sync_generate_table_description(self, context_str: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Synchronous table description generation."""
        # Use the existing AI service but with enhanced context
        result = self.ai_service.generate_table_description(
            schema_name=metadata['schema_name'],
            table_name=metadata['table_name'],
            columns=metadata['columns'],
            row_count=metadata.get('row_count')
        )
        
        return {
            'description': result.description,
            'suggested_name': result.suggested_name,
            'confidence_score': result.confidence_score,
            'reasoning': result.reasoning,
            'suggested_business_domain': result.suggested_business_domain,
            'suggested_is_pii': result.suggested_is_pii,
            'data_quality_warning': result.data_quality_warning,
            'model_used': result.model_used
        }
    
    def _sync_generate_column_description(self, context_str: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Synchronous column description generation."""
        # Extract profile data
        profile_data = {
            'cardinality': metadata.get('cardinality'),
            'null_percentage': metadata.get('null_percentage'),
            'top_values': list(metadata.get('top_values', {}).items()) if metadata.get('top_values') else [],
            'min_value': metadata.get('min_value'),
            'max_value': metadata.get('max_value')
        }
        
        sample_values = list(metadata.get('top_values', {}).keys())[:10] if metadata.get('top_values') else []
        
        result = self.ai_service.generate_column_description(
            table_name=metadata['table_name'],
            column_name=metadata['column_name'],
            data_type=metadata['data_type'],
            is_nullable=metadata['is_nullable'],
            profile_data=profile_data,
            sample_values=sample_values,
            table_context=metadata.get('table_description')  # Use table description as context!
        )
        
        return {
            'description': result.description,
            'suggested_name': result.suggested_name,
            'confidence_score': result.confidence_score,
            'reasoning': result.reasoning,
            'suggested_business_domain': result.suggested_business_domain,
            'suggested_is_pii': result.suggested_is_pii,
            'data_quality_warning': result.data_quality_warning,
            'model_used': result.model_used
        }
    
    def _build_table_context_string(self, context: EnhancedContext) -> str:
        """Build enhanced context string for table generation."""
        parts = [
            f"Table: {context.target_name}",
            f"Schema Business Domains: {', '.join(context.schema_patterns.business_domains)}",
            f"Naming Convention: {context.schema_patterns.naming_convention}",
        ]
        
        if context.relationships:
            parts.append("Relationships:")
            for rel in context.relationships[:5]:  # Limit to prevent token overflow
                parts.append(f"  - {rel.source_table}.{rel.source_column} -> {rel.target_table}.{rel.target_column}")
        
        if context.related_tables:
            parts.append(f"Related Tables: {', '.join(context.related_tables[:10])}")
            
        if context.domain_hints:
            parts.append(f"Inferred Domains: {', '.join(context.domain_hints)}")
            
        if context.similar_descriptions:
            parts.append("Similar Validated Descriptions (for reference):")
            for desc in context.similar_descriptions[:3]:
                parts.append(f"  - {desc['name']} (confidence: {desc['confidence']:.2f})")
        
        return "\\n".join(parts)
    
    def _build_column_context_string(self, context: EnhancedContext) -> str:
        """Build enhanced context string for column generation."""
        parts = [
            f"Column: {context.target_name}",
            f"Schema Patterns: {context.schema_patterns.naming_convention}",
        ]
        
        if context.basic_metadata.get('table_description'):
            parts.append(f"Table Purpose: {context.basic_metadata['table_description']}")
        
        if context.relationships:
            parts.append("Column Relationships:")
            for rel in context.relationships:
                if rel.source_column == context.basic_metadata['column_name']:
                    parts.append(f"  - References {rel.target_table}.{rel.target_column}")
                else:
                    parts.append(f"  - Referenced by {rel.source_table}.{rel.source_column}")
        
        if context.domain_hints:
            parts.append(f"Purpose Indicators: {', '.join(context.domain_hints)}")
            
        if context.similar_descriptions:
            parts.append("Similar Validated Descriptions (for reference):")
            for desc in context.similar_descriptions[:2]:
                parts.append(f"  - {desc['name']} (confidence: {desc['confidence']:.2f})")
        
        return "\\n".join(parts)
    
    async def _process_task(self, task: GenerationTask):
        """Process a single generation task."""
        async with self.rate_limit_semaphore:
            task.status = TaskStatus.RUNNING
            task.started_at = time.time()
            task.attempts += 1
            
            try:
                self._update_progress(current_task=task.target_name)
                
                result = await self._generate_description(task)
                task.result = result
                task.status = TaskStatus.COMPLETED
                task.completed_at = time.time()
                
                # Save to database
                await self._save_result(task)
                
                self.completed_tasks.append(task)
                logger.info(f"Completed {task.target_name} in {task.completed_at - task.started_at:.1f}s")
                
            except Exception as e:
                task.error = str(e)
                if task.attempts < self.max_retries:
                    task.status = TaskStatus.PENDING
                    logger.warning(f"Task {task.target_name} failed (attempt {task.attempts}/{self.max_retries}): {e}")
                    # Re-queue for retry
                    await self.task_queue.put(task)
                else:
                    task.status = TaskStatus.FAILED
                    self.failed_tasks.append(task)
                    logger.error(f"Task {task.target_name} failed permanently: {e}")
            
            finally:
                self._update_progress()
    
    async def _save_result(self, task: GenerationTask):
        """Save generation result to database."""
        if not task.result:
            return
            
        # This would need to be run in a thread to avoid blocking
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as executor:
            await loop.run_in_executor(executor, self._sync_save_result, task)
    
    def _sync_save_result(self, task: GenerationTask):
        """Synchronously save result to database."""
        result = task.result
        
        if task.target_type == 'table':
            description = AIDescription(
                table_id=task.target_id,
                description=result['description'],
                suggested_name=result['suggested_name'],
                confidence_score=result['confidence_score'],
                context_used="enhanced_context_v1",
                reasoning=result['reasoning'],
                suggested_business_domain=result['suggested_business_domain'],
                suggested_is_pii=result['suggested_is_pii'],
                suggested_data_quality_warning=result['data_quality_warning'],
                model_used=result['model_used'],
                prompt_version="enhanced_v1.0"
            )
        else:
            description = AIDescription(
                column_id=task.target_id,
                description=result['description'],
                suggested_name=result['suggested_name'],
                confidence_score=result['confidence_score'],
                context_used="enhanced_context_v1",
                reasoning=result['reasoning'],
                suggested_business_domain=result['suggested_business_domain'],
                suggested_is_pii=result['suggested_is_pii'],
                suggested_data_quality_warning=result['data_quality_warning'],
                model_used=result['model_used'],
                prompt_version="enhanced_v1.0"
            )
        
        self.db.add(description)
        self.db.commit()
    
    def _update_progress(self, current_task: Optional[str] = None):
        """Update and emit progress information."""
        if not self.progress_callback:
            return
            
        total = len(self.tasks)
        completed = len(self.completed_tasks)
        failed = len(self.failed_tasks)
        running = len([t for t in self.tasks.values() if t.status == TaskStatus.RUNNING])
        pending = total - completed - failed - running
        
        # Estimate remaining time
        estimated_remaining = None
        if completed > 0 and self.start_time:
            elapsed = time.time() - self.start_time
            avg_time_per_task = elapsed / completed
            estimated_remaining = avg_time_per_task * pending
        
        progress = GenerationProgress(
            total_tasks=total,
            completed=completed,
            failed=failed,
            running=running,
            pending=pending,
            estimated_remaining_time=estimated_remaining,
            current_task=current_task
        )
        
        self.progress_callback(progress)
    
    async def generate_descriptions(
        self,
        tables: List[Table],
        selected_column_ids: Optional[Set[int]] = None
    ) -> Dict[str, Any]:
        """Main method to generate descriptions with dependency management and concurrency."""
        self.start_time = time.time()
        
        # Build dependency graph
        dependencies = self._build_dependency_graph(tables, selected_column_ids)
        
        # Create tasks
        self.tasks = {}
        
        for table in tables:
            # Create table task
            table_task_id = self._create_task_id('table', table.id)
            table_context = self.context_builder.build_table_context(table)
            
            table_task = GenerationTask(
                id=table_task_id,
                target_type='table',
                target_id=table.id,
                target_name=f"{table.schema_name}.{table.table_name}",
                priority=self._calculate_priority('table', table),
                dependencies=dependencies.get(table_task_id, set()),
                context=table_context
            )
            self.tasks[table_task_id] = table_task
            
            # Create column tasks
            for column in table.columns:
                if selected_column_ids is None or column.id in selected_column_ids:
                    column_task_id = self._create_task_id('column', column.id)
                    column_context = self.context_builder.build_column_context(column)
                    
                    column_task = GenerationTask(
                        id=column_task_id,
                        target_type='column',
                        target_id=column.id,
                        target_name=f"{table.schema_name}.{table.table_name}.{column.column_name}",
                        priority=self._calculate_priority('column', table, column),
                        dependencies=dependencies.get(column_task_id, set()),
                        context=column_context
                    )
                    self.tasks[column_task_id] = column_task
        
        # Sort tasks by priority (higher priority first)
        sorted_tasks = sorted(self.tasks.values(), key=lambda t: t.priority, reverse=True)
        
        # Add ready tasks to queue (those with no dependencies)
        ready_tasks = [task for task in sorted_tasks if not task.dependencies]
        for task in ready_tasks:
            await self.task_queue.put(task)
        
        # Process tasks concurrently
        workers = [asyncio.create_task(self._worker()) for _ in range(self.max_concurrent)]
        
        # Wait for all tasks to complete
        while not self.task_queue.empty() or any(t.status == TaskStatus.RUNNING for t in self.tasks.values()):
            await asyncio.sleep(0.1)
            
            # Check for newly available tasks
            for task in self.tasks.values():
                if (task.status == TaskStatus.PENDING and 
                    task not in ready_tasks and
                    all(self.tasks[dep_id].status == TaskStatus.COMPLETED for dep_id in task.dependencies)):
                    ready_tasks.append(task)
                    await self.task_queue.put(task)
        
        # Cancel workers
        for worker in workers:
            worker.cancel()
        
        total_time = time.time() - self.start_time
        
        return {
            'descriptions_generated': len(self.completed_tasks),
            'failed_tasks': len(self.failed_tasks),
            'total_time': total_time,
            'tasks_per_second': len(self.completed_tasks) / total_time if total_time > 0 else 0
        }
    
    async def _worker(self):
        """Worker coroutine to process tasks from the queue."""
        while True:
            try:
                task = await self.task_queue.get()
                await self._process_task(task)
                self.task_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker error: {e}")