"""Core business logic for managing the data catalog."""

import logging
import os
import asyncio
from typing import List, Optional, Dict, Any, Set, Callable
from sqlalchemy.orm import Session
from datetime import datetime

from ..models import DataSource, Table, Column, AIDescription, ValidationStatus
from ..services import DataProfiler, AIService
from ..services.ai_service import LLMProvider
from ..services.multi_db_connector import MultiDatabaseConnector, DatabaseType
from ..services.enhanced_context_builder import EnhancedContextBuilder
from ..services.async_generation_engine import AsyncGenerationEngine, GenerationProgress

logger = logging.getLogger(__name__)


class CatalogManager:
    """Manages the data catalog operations."""
    
    def __init__(self, db_session: Session):
        """Initialize with database session."""
        self.db = db_session
        self.ai_service = None  # Initialize lazily when needed
        
    def _get_ai_service(self) -> AIService:
        """Get or create AI service instance."""
        if self.ai_service is None:
            # Try OpenAI first, then Anthropic
            if os.getenv("OPENAI_API_KEY"):
                self.ai_service = AIService(LLMProvider.OPENAI)
            elif os.getenv("ANTHROPIC_API_KEY"):
                self.ai_service = AIService(LLMProvider.ANTHROPIC)
            else:
                raise ValueError("Either OPENAI_API_KEY or ANTHROPIC_API_KEY environment variable must be set for AI description generation")
        return self.ai_service
    
    def add_data_source(self, name: str, connection_string: str, 
                       database_type: str = "postgresql") -> DataSource:
        """Add a new data source to the catalog."""
        # Validate database type
        try:
            db_type = DatabaseType(database_type)
        except ValueError:
            raise ValueError(f"Unsupported database type: {database_type}")
        
        # Validate connection string format
        if not MultiDatabaseConnector.validate_connection_string(connection_string, db_type):
            example = MultiDatabaseConnector.get_connection_string_example(db_type)
            raise ValueError(f"Invalid connection string format. Example: {example}")
        
        # Test connection first
        connector = MultiDatabaseConnector(connection_string, db_type)
        if not connector.connect():
            raise ValueError("Cannot connect to database")
        
        data_source = DataSource(
            name=name,
            connection_string=connection_string,
            database_type=database_type
        )
        
        self.db.add(data_source)
        self.db.commit()
        self.db.refresh(data_source)
        
        logger.info(f"Added data source: {name}")
        return data_source
    
    def remove_data_source(self, data_source_id: int) -> Dict[str, Any]:
        """Remove a data source and all its associated data."""
        from ..models import AIDescription
        
        data_source = self.db.query(DataSource).filter(
            DataSource.id == data_source_id
        ).first()
        
        if not data_source:
            raise ValueError(f"Data source {data_source_id} not found")
        
        # Get counts for reporting before deletion
        tables_count = len(data_source.tables)
        columns_count = sum(len(table.columns) for table in data_source.tables)
        descriptions_count = sum(
            len(table.ai_descriptions) + sum(len(col.ai_descriptions) for col in table.columns)
            for table in data_source.tables
        )
        
        name = data_source.name
        
        try:
            # Delete in the correct order to avoid foreign key constraint issues
            
            # 1. Delete all AI descriptions first
            for table in data_source.tables:
                # Delete table descriptions
                self.db.query(AIDescription).filter(
                    AIDescription.table_id == table.id
                ).delete()
                
                # Delete column descriptions
                for column in table.columns:
                    self.db.query(AIDescription).filter(
                        AIDescription.column_id == column.id
                    ).delete()
            
            # 2. Delete columns
            for table in data_source.tables:
                for column in table.columns:
                    self.db.delete(column)
            
            # 3. Delete tables
            for table in data_source.tables:
                self.db.delete(table)
            
            # 4. Finally delete the data source
            self.db.delete(data_source)
            
            # Commit all deletions
            self.db.commit()
            
            logger.info(f"Removed data source: {name} ({tables_count} tables, {columns_count} columns, {descriptions_count} descriptions)")
            return {
                "name": name,
                "tables_removed": tables_count,
                "columns_removed": columns_count,
                "descriptions_removed": descriptions_count
            }
            
        except Exception as e:
            # Rollback on any error
            self.db.rollback()
            logger.error(f"Failed to remove data source {name}: {e}")
            raise
    
    def discover_schema(self, data_source_id: int, 
                       schemas: List[str] = None) -> Dict[str, int]:
        """Discover and catalog tables and columns from a data source."""
        data_source = self.db.query(DataSource).filter(
            DataSource.id == data_source_id
        ).first()
        
        if not data_source:
            raise ValueError(f"Data source {data_source_id} not found")
        
        # Get database type
        try:
            db_type = DatabaseType(data_source.database_type)
        except ValueError:
            raise ValueError(f"Unsupported database type: {data_source.database_type}")
        
        connector = MultiDatabaseConnector(data_source.connection_string, db_type)
        if not connector.connect():
            raise ValueError("Cannot connect to database")
        
        profiler = DataProfiler(connector.engine)
        
        # Get tables
        tables_metadata = connector.get_tables(schemas)
        tables_added = 0
        columns_added = 0
        
        for table_meta in tables_metadata:
            # Check if table already exists
            existing_table = self.db.query(Table).filter(
                Table.data_source_id == data_source_id,
                Table.schema_name == table_meta.schema_name,
                Table.table_name == table_meta.table_name
            ).first()
            
            if existing_table:
                table = existing_table
                table.row_count = table_meta.row_count
                table.last_profiled_at = datetime.utcnow()
            else:
                table = Table(
                    data_source_id=data_source_id,
                    schema_name=table_meta.schema_name,
                    table_name=table_meta.table_name,
                    table_type=table_meta.table_type,
                    row_count=table_meta.row_count,
                    last_profiled_at=datetime.utcnow()
                )
                self.db.add(table)
                tables_added += 1
            
            self.db.commit()
            self.db.refresh(table)
            
            # Get columns for this table
            columns_metadata = connector.get_columns(
                table_meta.schema_name, 
                table_meta.table_name
            )
            
            for col_meta in columns_metadata:
                # Check if column already exists
                existing_column = self.db.query(Column).filter(
                    Column.table_id == table.id,
                    Column.column_name == col_meta.column_name
                ).first()
                
                if not existing_column:
                    column = Column(
                        table_id=table.id,
                        column_name=col_meta.column_name,
                        data_type=col_meta.data_type,
                        is_nullable=col_meta.is_nullable,
                        column_default=col_meta.column_default,
                        character_maximum_length=col_meta.character_maximum_length,
                        numeric_precision=col_meta.numeric_precision,
                        numeric_scale=col_meta.numeric_scale,
                        ordinal_position=col_meta.ordinal_position
                    )
                    self.db.add(column)
                    columns_added += 1
                else:
                    column = existing_column
                
                # Profile the column
                try:
                    profile = profiler.profile_column(
                        table_meta.schema_name,
                        table_meta.table_name, 
                        col_meta.column_name
                    )
                    
                    # Update column with profile data
                    column.cardinality = profile.cardinality
                    column.null_percentage = profile.null_percentage
                    column.top_values = dict(profile.top_values[:10])  # Store as JSON
                    column.min_value = profile.min_value
                    column.max_value = profile.max_value
                    column.avg_value = profile.avg_value
                    column.std_dev = profile.std_dev
                    column.last_profiled_at = datetime.utcnow()
                    
                    # Auto-classify column
                    classification = profiler.classify_column(
                        col_meta.column_name,
                        col_meta.data_type,
                        profile
                    )
                    
                    column.is_pii = classification['is_pii']
                    column.is_key = classification['is_key']
                    column.business_domain = classification['business_domain']
                    
                except Exception as e:
                    logger.warning(f"Failed to profile column {col_meta.column_name}: {e}")
            
            self.db.commit()
        
        logger.info(f"Schema discovery complete: {tables_added} tables, {columns_added} columns added")
        return {"tables_added": tables_added, "columns_added": columns_added}
    
    def generate_descriptions(self, data_source_id: int, 
                            table_id: Optional[int] = None,
                            table_ids: Optional[List[int]] = None,
                            column_ids: Optional[List[int]] = None,
                            progress_callback=None) -> Dict[str, int]:
        """Generate AI descriptions for tables and columns."""
        query = self.db.query(Table).filter(Table.data_source_id == data_source_id)
        
        if table_id:
            query = query.filter(Table.id == table_id)
        elif table_ids:
            query = query.filter(Table.id.in_(table_ids))
        
        tables = query.all()
        descriptions_generated = 0
        total_tables = len(tables)
        total_columns = sum(len(table.columns) for table in tables)
        processed_items = 0
        
        if progress_callback:
            progress_callback(f"Starting generation for {total_tables} tables with {total_columns} columns", 0, total_tables + total_columns)
        
        for table_idx, table in enumerate(tables):
            # Generate table description
            columns_data = []
            for col in table.columns:
                columns_data.append({
                    'column_name': col.column_name,
                    'data_type': col.data_type,
                    'description': getattr(col.ai_descriptions[0], 'description', None) 
                                 if col.ai_descriptions else None
                })
            
            if progress_callback:
                progress_callback(f"Generating description for table {table.table_name}", processed_items, total_tables + total_columns)
            
            try:
                ai_service = self._get_ai_service()
                table_result = ai_service.generate_table_description(
                    table.schema_name,
                    table.table_name,
                    columns_data,
                    table.row_count
                )
                
                # Save table description
                table_desc = AIDescription(
                    table_id=table.id,
                    description=table_result.description,
                    suggested_name=table_result.suggested_name,
                    confidence_score=table_result.confidence_score,
                    context_used="",  # Could store the context here
                    reasoning=table_result.reasoning,
                    suggested_business_domain=table_result.suggested_business_domain,
                    suggested_is_pii=table_result.suggested_is_pii,
                    suggested_data_quality_warning=table_result.data_quality_warning,
                    model_used=table_result.model_used,
                    prompt_version="v1.0"
                )
                
                self.db.add(table_desc)
                descriptions_generated += 1
                processed_items += 1
                
            except Exception as e:
                logger.error(f"Failed to generate table description for {table.full_name}: {e}")
            
            # Generate column descriptions
            for column in table.columns:
                # Skip if column not in selection (when column_ids is specified)
                if column_ids and column.id not in column_ids:
                    continue
                
                # Skip if already has description
                existing_desc = self.db.query(AIDescription).filter(
                    AIDescription.column_id == column.id,
                    AIDescription.status != ValidationStatus.REJECTED
                ).first()
                
                if existing_desc:
                    continue
                
                if progress_callback:
                    progress_callback(f"Generating description for column {table.table_name}.{column.column_name}", processed_items, total_tables + total_columns)
                
                try:
                    profile_data = {
                        'cardinality': column.cardinality,
                        'null_percentage': column.null_percentage,
                        'top_values': list(column.top_values.items()) if column.top_values else [],
                        'min_value': column.min_value,
                        'max_value': column.max_value
                    }
                    
                    # Get sample values (simplified for MVP)
                    sample_values = []
                    if column.top_values:
                        sample_values = list(column.top_values.keys())[:10]
                    
                    ai_service = self._get_ai_service()
                    col_result = ai_service.generate_column_description(
                        table.table_name,
                        column.column_name,
                        column.data_type,
                        column.is_nullable,
                        profile_data,
                        sample_values
                    )
                    
                    # Save column description
                    col_desc = AIDescription(
                        column_id=column.id,
                        description=col_result.description,
                        suggested_name=col_result.suggested_name,
                        confidence_score=col_result.confidence_score,
                        context_used="",
                        reasoning=col_result.reasoning,
                        suggested_business_domain=col_result.suggested_business_domain,
                        suggested_is_pii=col_result.suggested_is_pii,
                        suggested_data_quality_warning=col_result.data_quality_warning,
                        model_used=col_result.model_used,
                        prompt_version="v1.0"
                    )
                    
                    self.db.add(col_desc)
                    descriptions_generated += 1
                    processed_items += 1
                    
                except Exception as e:
                    logger.error(f"Failed to generate column description for {column.column_name}: {e}")
                    processed_items += 1
        
        self.db.commit()
        
        if progress_callback:
            progress_callback(f"Generation complete! Generated {descriptions_generated} descriptions", total_tables + total_columns, total_tables + total_columns)
        
        logger.info(f"Generated {descriptions_generated} descriptions")
        return {"descriptions_generated": descriptions_generated}
    
    async def generate_descriptions_enhanced(
        self,
        data_source_id: int,
        table_ids: Optional[List[int]] = None,
        column_ids: Optional[Set[int]] = None,
        max_concurrent: int = 5,
        rate_limit_rpm: int = 60,
        progress_callback: Optional[Callable[[GenerationProgress], None]] = None,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Generate AI descriptions using enhanced context and async processing.
        
        This method provides:
        - Rich context assembly with relationships and business domain inference
        - Concurrent processing for better performance
        - Smart dependency management (tables before columns)
        - Rate limiting and caching
        - Real-time progress updates
        """
        # Get data source
        data_source = self.db.query(DataSource).filter(
            DataSource.id == data_source_id
        ).first()
        
        if not data_source:
            raise ValueError(f"Data source {data_source_id} not found")
        
        # Get tables to process
        query = self.db.query(Table).filter(Table.data_source_id == data_source_id)
        if table_ids:
            query = query.filter(Table.id.in_(table_ids))
        tables = query.all()
        
        if not tables:
            return {"descriptions_generated": 0, "message": "No tables found to process"}
        
        # Initialize enhanced components
        try:
            ai_service = self._get_ai_service()
            context_builder = EnhancedContextBuilder(self.db, data_source)
            
            generation_engine = AsyncGenerationEngine(
                db_session=self.db,
                ai_service=ai_service,
                context_builder=context_builder,
                max_concurrent=max_concurrent,
                rate_limit_rpm=rate_limit_rpm,
                cache_enabled=use_cache
            )
            
            if progress_callback:
                generation_engine.set_progress_callback(progress_callback)
            
            # Filter columns by existing descriptions if not explicitly selected
            if column_ids is None:
                # Only generate for columns that don't have approved descriptions
                column_ids = set()
                for table in tables:
                    for column in table.columns:
                        existing_desc = self.db.query(AIDescription).filter(
                            AIDescription.column_id == column.id,
                            AIDescription.status != ValidationStatus.REJECTED
                        ).first()
                        if not existing_desc:
                            column_ids.add(column.id)
            
            # Run async generation
            logger.info(f"Starting enhanced generation for {len(tables)} tables with {len(column_ids)} columns")
            result = await generation_engine.generate_descriptions(tables, column_ids)
            
            # Update statistics
            result.update({
                "tables_processed": len(tables),
                "columns_processed": len(column_ids),
                "enhancement_features": [
                    "relationship_context",
                    "business_domain_inference", 
                    "dependency_management",
                    "concurrent_processing",
                    "smart_caching"
                ]
            })
            
            logger.info(f"Enhanced generation complete: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Enhanced generation failed: {e}")
            raise
    
    def generate_descriptions_sync_wrapper(
        self,
        data_source_id: int,
        table_ids: Optional[List[int]] = None,
        column_ids: Optional[Set[int]] = None,
        max_concurrent: int = 5,
        rate_limit_rpm: int = 60,
        progress_callback: Optional[Callable[[GenerationProgress], None]] = None,
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Synchronous wrapper for enhanced generation.
        Use this from non-async contexts.
        """
        try:
            # Create new event loop if none exists
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    raise RuntimeError("Event loop is closed")
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Run the async method
            return loop.run_until_complete(
                self.generate_descriptions_enhanced(
                    data_source_id=data_source_id,
                    table_ids=table_ids,
                    column_ids=column_ids,
                    max_concurrent=max_concurrent,
                    rate_limit_rpm=rate_limit_rpm,
                    progress_callback=progress_callback,
                    use_cache=use_cache
                )
            )
        except Exception as e:
            logger.error(f"Sync wrapper failed: {e}")
            # Fallback to original method
            logger.info("Falling back to original generation method")
            return self.generate_descriptions(
                data_source_id=data_source_id,
                table_ids=table_ids,
                column_ids=column_ids
            )
    
    def get_tables(self, data_source_id: int) -> List[Dict[str, Any]]:
        """Get all tables for a data source."""
        tables = self.db.query(Table).filter(
            Table.data_source_id == data_source_id
        ).all()
        
        result = []
        for table in tables:
            table_desc = self.db.query(AIDescription).filter(
                AIDescription.table_id == table.id
            ).first()
            
            result.append({
                'id': table.id,
                'schema_name': table.schema_name,
                'table_name': table.table_name,
                'full_name': table.full_name,
                'table_type': table.table_type,
                'row_count': table.row_count,
                'column_count': len(table.columns),
                'description': (table_desc.final_description or table_desc.description) if table_desc else None,
                'ai_confidence': table_desc.confidence_score if table_desc else None,
                'last_profiled_at': table.last_profiled_at
            })
        
        return result
    
    def get_table_details(self, table_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed information about a table including columns."""
        table = self.db.query(Table).filter(Table.id == table_id).first()
        if not table:
            return None
        
        table_desc = self.db.query(AIDescription).filter(
            AIDescription.table_id == table.id
        ).first()
        
        columns = []
        for col in table.columns:
            col_desc = self.db.query(AIDescription).filter(
                AIDescription.column_id == col.id
            ).first()
            
            columns.append({
                'id': col.id,
                'name': col.column_name,
                'data_type': col.data_type,
                'is_nullable': col.is_nullable,
                'is_pii': col.is_pii,
                'is_key': col.is_key,
                'cardinality': col.cardinality,
                'null_percentage': col.null_percentage,
                'description': (col_desc.final_description or col_desc.description) if col_desc else None,
                'ai_confidence': col_desc.confidence_score if col_desc else None,
                'validation_status': col_desc.status.value if col_desc else 'pending',
                'description_id': col_desc.id if col_desc else None
            })
        
        return {
            'id': table.id,
            'data_source_id': table.data_source_id,
            'schema_name': table.schema_name,
            'table_name': table.table_name,
            'full_name': table.full_name,
            'table_type': table.table_type,
            'row_count': table.row_count,
            'description': (table_desc.final_description or table_desc.description) if table_desc else None,
            'ai_confidence': table_desc.confidence_score if table_desc else None,
            'validation_status': table_desc.status.value if table_desc else 'pending',
            'description_id': table_desc.id if table_desc else None,
            'columns': columns
        }
    
    def validate_description(self, description_id: int, action: str, feedback: Optional[str] = None) -> Dict[str, Any]:
        """Validate an AI-generated description."""
        description = self.db.query(AIDescription).filter(
            AIDescription.id == description_id
        ).first()
        
        if not description:
            raise ValueError(f"Description {description_id} not found")
        
        if action == "approve":
            description.status = ValidationStatus.APPROVED
            description.final_description = description.description
            description.validated_at = datetime.utcnow()
            description.human_feedback = feedback
            
        elif action == "edit":
            if not feedback:
                raise ValueError("Feedback is required for edit action")
            description.status = ValidationStatus.EDITED
            description.final_description = feedback
            description.validated_at = datetime.utcnow()
            description.human_feedback = "Description edited by human"
            
        elif action == "reject":
            description.status = ValidationStatus.REJECTED
            description.validated_at = datetime.utcnow()
            description.human_feedback = feedback or "Rejected by human reviewer"
            
        else:
            raise ValueError(f"Invalid action: {action}")
        
        self.db.commit()
        
        return {
            "description_id": description_id,
            "new_status": description.status.value,
            "validated_at": description.validated_at
        }
    
    def get_pending_descriptions(self) -> List[Dict[str, Any]]:
        """Get all descriptions pending validation."""
        descriptions = self.db.query(AIDescription).filter(
            AIDescription.status == ValidationStatus.PENDING
        ).all()
        
        result = []
        for desc in descriptions:
            item = {
                'id': desc.id,
                'description': desc.description,
                'confidence_score': desc.confidence_score,
                'reasoning': desc.reasoning,
                'created_at': desc.created_at
            }
            
            if desc.table_id:
                table = self.db.query(Table).filter(Table.id == desc.table_id).first()
                item['type'] = 'table'
                item['target_name'] = f"{table.schema_name}.{table.table_name}"
            elif desc.column_id:
                column = self.db.query(Column).filter(Column.id == desc.column_id).first()
                table = column.table
                item['type'] = 'column'
                item['target_name'] = f"{table.schema_name}.{table.table_name}.{column.column_name}"
            
            result.append(item)
        
        return result