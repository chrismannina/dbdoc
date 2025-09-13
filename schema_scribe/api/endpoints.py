"""Enhanced API endpoints with pagination, filtering, and new features."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
from typing import List, Optional
import re
import uuid
from datetime import datetime

from ..models.base import get_db
from ..models import DataSource, Table, Column, TableFilter, UserContext, Relationship
from ..core import CatalogManager
from ..services.job_manager import JobManager, JobType, create_job, start_job, get_job, list_jobs
from .schemas import (
    DataSourceCreate, DataSourceUpdate, DataSourceResponse,
    TableResponse, PaginatedTableResponse, TableListParams,
    TableFilterCreate, TableFilterBulkUpdate, TableFilterResponse,
    UserContextCreate, UserContextUpdate, UserContextResponse,
    ColumnResponse,
    DiscoveryParams, DiscoveryResponse,
    GenerationParams, GenerationResponse,
    JobStatus, JobStatusResponse,
    ERDRequest, ERDResponse
)

router = APIRouter(prefix="/api/v2", tags=["Enhanced API"])

# Use the global job manager
from ..services.job_manager import job_manager


# Data Source endpoints
@router.post("/data-sources", response_model=DataSourceResponse)
async def create_data_source(data_source: DataSourceCreate, db: Session = Depends(get_db)):
    """Create a new data source with enhanced filtering options."""
    catalog_manager = CatalogManager(db)
    
    # Create data source with new fields
    new_ds = DataSource(
        name=data_source.name,
        connection_string=data_source.connection_string,
        database_type=data_source.database_type,
        description=data_source.description,
        databases=data_source.databases,
        included_schemas=data_source.included_schemas,
        excluded_schemas=data_source.excluded_schemas,
        included_tables_pattern=data_source.included_tables_pattern,
        excluded_tables_pattern=data_source.excluded_tables_pattern,
        auto_profile=data_source.auto_profile,
        sample_size=data_source.sample_size
    )
    
    db.add(new_ds)
    db.commit()
    db.refresh(new_ds)
    
    # Get table count
    table_count = db.query(Table).filter(Table.data_source_id == new_ds.id).count()
    
    response = DataSourceResponse.from_orm(new_ds)
    response.table_count = table_count
    return response


@router.patch("/data-sources/{data_source_id}", response_model=DataSourceResponse)
async def update_data_source(
    data_source_id: int,
    update: DataSourceUpdate,
    db: Session = Depends(get_db)
):
    """Update data source settings."""
    ds = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not ds:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Update fields if provided
    update_data = update.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(ds, field, value)
    
    db.commit()
    db.refresh(ds)
    
    table_count = db.query(Table).filter(Table.data_source_id == ds.id).count()
    response = DataSourceResponse.from_orm(ds)
    response.table_count = table_count
    return response


# Table endpoints with pagination
@router.get("/tables", response_model=PaginatedTableResponse)
async def list_tables(
    data_source_id: int = Query(..., description="Data source ID"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    search: Optional[str] = Query(None, description="Search in table names"),
    schema_filter: Optional[str] = Query(None, description="Filter by schema"),
    has_descriptions: Optional[bool] = Query(None, description="Filter by description status"),
    is_included: Optional[bool] = Query(None, description="Filter by inclusion status"),
    priority: Optional[str] = Query(None, description="Filter by priority"),
    db: Session = Depends(get_db)
):
    """List tables with pagination and filtering."""
    query = db.query(Table).filter(Table.data_source_id == data_source_id)
    
    # Apply filters
    if search:
        query = query.filter(Table.table_name.ilike(f"%{search}%"))
    
    if schema_filter:
        query = query.filter(Table.schema_name == schema_filter)
    
    # Join with TableFilter for inclusion/priority filtering
    if is_included is not None or priority is not None:
        query = query.outerjoin(TableFilter)
        
        if is_included is not None:
            if is_included:
                query = query.filter(or_(
                    TableFilter.is_included == True,
                    TableFilter.id == None  # No filter means included by default
                ))
            else:
                query = query.filter(TableFilter.is_included == False)
        
        if priority:
            query = query.filter(TableFilter.priority == priority)
    
    # Get total count
    total = query.count()
    
    # Apply pagination
    tables = query.limit(limit).offset(offset).all()
    
    # Build response
    items = []
    for table in tables:
        # Get additional info
        column_count = db.query(Column).filter(Column.table_id == table.id).count()
        has_description = db.query(func.count()).select_from(
            db.query(Column).filter(
                Column.table_id == table.id
            ).join(
                Column.ai_descriptions
            ).subquery()
        ).scalar() > 0
        
        # Get filter info
        table_filter = db.query(TableFilter).filter(
            TableFilter.table_id == table.id
        ).first()
        
        response = TableResponse.from_orm(table)
        response.column_count = column_count
        response.has_description = has_description
        response.is_included = table_filter.is_included if table_filter else True
        response.priority = table_filter.priority if table_filter else "normal"
        
        items.append(response)
    
    return PaginatedTableResponse(
        items=items,
        total=total,
        limit=limit,
        offset=offset,
        has_more=(offset + limit) < total
    )


# Table Filter endpoints
@router.post("/table-filters", response_model=TableFilterResponse)
async def create_table_filter(
    data_source_id: int,
    filter_create: TableFilterCreate,
    db: Session = Depends(get_db)
):
    """Create or update a table filter."""
    # Check if filter already exists
    existing = db.query(TableFilter).filter(
        and_(
            TableFilter.data_source_id == data_source_id,
            TableFilter.table_id == filter_create.table_id
        )
    ).first()
    
    if existing:
        # Update existing
        existing.is_included = filter_create.is_included
        existing.priority = filter_create.priority.value
        existing.reason = filter_create.reason
        existing.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(existing)
        return TableFilterResponse.from_orm(existing)
    else:
        # Create new
        new_filter = TableFilter(
            data_source_id=data_source_id,
            table_id=filter_create.table_id,
            is_included=filter_create.is_included,
            priority=filter_create.priority.value,
            reason=filter_create.reason
        )
        db.add(new_filter)
        db.commit()
        db.refresh(new_filter)
        return TableFilterResponse.from_orm(new_filter)


@router.post("/table-filters/bulk", response_model=List[TableFilterResponse])
async def bulk_update_table_filters(
    data_source_id: int,
    bulk_update: TableFilterBulkUpdate,
    db: Session = Depends(get_db)
):
    """Bulk update table filters."""
    updated_filters = []
    
    for table_id in bulk_update.table_ids:
        # Check if filter exists
        existing = db.query(TableFilter).filter(
            and_(
                TableFilter.data_source_id == data_source_id,
                TableFilter.table_id == table_id
            )
        ).first()
        
        if existing:
            # Update existing
            if bulk_update.is_included is not None:
                existing.is_included = bulk_update.is_included
            if bulk_update.priority is not None:
                existing.priority = bulk_update.priority.value
            if bulk_update.reason is not None:
                existing.reason = bulk_update.reason
            existing.updated_at = datetime.utcnow()
            updated_filters.append(existing)
        else:
            # Create new
            new_filter = TableFilter(
                data_source_id=data_source_id,
                table_id=table_id,
                is_included=bulk_update.is_included if bulk_update.is_included is not None else True,
                priority=bulk_update.priority.value if bulk_update.priority else "normal",
                reason=bulk_update.reason
            )
            db.add(new_filter)
            updated_filters.append(new_filter)
    
    db.commit()
    
    # Refresh all
    for f in updated_filters:
        db.refresh(f)
    
    return [TableFilterResponse.from_orm(f) for f in updated_filters]


# User Context endpoints
@router.post("/user-context", response_model=UserContextResponse)
async def create_user_context(
    context: UserContextCreate,
    db: Session = Depends(get_db)
):
    """Create or update user context for a table or column."""
    # Check if context already exists
    query = db.query(UserContext)
    
    if context.table_id:
        query = query.filter(UserContext.table_id == context.table_id)
    if context.column_id:
        query = query.filter(UserContext.column_id == context.column_id)
    
    existing = query.first()
    
    if existing:
        # Update existing
        for field, value in context.dict(exclude_unset=True).items():
            if field not in ['table_id', 'column_id']:
                setattr(existing, field, value)
        existing.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(existing)
        return UserContextResponse.from_orm(existing)
    else:
        # Create new
        new_context = UserContext(**context.dict())
        db.add(new_context)
        db.commit()
        db.refresh(new_context)
        return UserContextResponse.from_orm(new_context)


@router.get("/user-context/table/{table_id}", response_model=Optional[UserContextResponse])
async def get_table_context(table_id: int, db: Session = Depends(get_db)):
    """Get user context for a table."""
    context = db.query(UserContext).filter(UserContext.table_id == table_id).first()
    if context:
        return UserContextResponse.from_orm(context)
    return None


@router.get("/user-context/column/{column_id}", response_model=Optional[UserContextResponse])
async def get_column_context(column_id: int, db: Session = Depends(get_db)):
    """Get user context for a column."""
    context = db.query(UserContext).filter(UserContext.column_id == column_id).first()
    if context:
        return UserContextResponse.from_orm(context)
    return None


# Enhanced Discovery endpoint
@router.post("/data-sources/{data_source_id}/discover", response_model=DiscoveryResponse)
async def discover_schema_enhanced(
    data_source_id: int,
    params: DiscoveryParams,
    db: Session = Depends(get_db)
):
    """Discover schema with enhanced filtering."""
    catalog_manager = CatalogManager(db)
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    # Apply schema filters from data source settings
    schemas_to_discover = params.schemas
    if not schemas_to_discover and data_source.included_schemas:
        schemas_to_discover = data_source.included_schemas
    
    # Discover schema
    start_time = datetime.utcnow()
    result = catalog_manager.discover_schema(
        data_source_id=data_source_id,
        schemas=schemas_to_discover
    )
    
    # Apply table pattern filters
    if data_source.included_tables_pattern or data_source.excluded_tables_pattern:
        tables = db.query(Table).filter(Table.data_source_id == data_source_id).all()
        
        for table in tables:
            include = True
            
            # Check inclusion pattern
            if data_source.included_tables_pattern:
                include = bool(re.match(data_source.included_tables_pattern, table.table_name))
            
            # Check exclusion pattern
            if data_source.excluded_tables_pattern and re.match(data_source.excluded_tables_pattern, table.table_name):
                include = False
            
            # Create filter if needed
            if not include:
                table_filter = TableFilter(
                    data_source_id=data_source_id,
                    table_id=table.id,
                    is_included=False,
                    reason="Excluded by pattern filter"
                )
                db.add(table_filter)
        
        db.commit()
    
    # Auto-profile if enabled
    if params.auto_profile and data_source.auto_profile:
        # This would trigger profiling - implementation depends on your profiling service
        pass
    
    duration = (datetime.utcnow() - start_time).total_seconds()
    
    return DiscoveryResponse(
        tables_discovered=result.get('tables_discovered', 0),
        columns_discovered=result.get('columns_discovered', 0),
        schemas_processed=schemas_to_discover or [],
        duration_seconds=duration
    )


# Enhanced Generation endpoint with job tracking
@router.post("/data-sources/{data_source_id}/generate", response_model=GenerationResponse)
async def generate_descriptions_async(
    data_source_id: int,
    params: GenerationParams,
    db: Session = Depends(get_db)
):
    """Start async generation job with user context."""
    # Get tables to process
    query = db.query(Table).filter(Table.data_source_id == data_source_id)
    
    if params.table_ids:
        query = query.filter(Table.id.in_(params.table_ids))
    else:
        # Only process included tables
        query = query.outerjoin(TableFilter).filter(
            or_(
                TableFilter.is_included == True,
                TableFilter.id == None
            )
        )
    
    tables = query.all()
    
    # Create job
    job_id = create_job(
        job_type=JobType.DESCRIPTION_GENERATION,
        title=f"Generate Descriptions for {len(tables)} tables",
        description=f"Generating AI descriptions for data source {data_source_id}",
        total_items=len(tables),
        metadata={
            "data_source_id": data_source_id,
            "table_ids": [t.id for t in tables],
            "use_user_context": params.use_user_context,
            "include_columns": params.include_columns
        }
    )
    
    # Start the job
    def generation_job(progress_callback, db_session, data_source_id, table_ids, params):
        """Job function for description generation."""
        catalog_manager = CatalogManager(db_session)
        
        try:
            result = catalog_manager.generate_descriptions_sync_wrapper(
                data_source_id=data_source_id,
                table_ids=table_ids,
                max_concurrent=5,
                rate_limit_rpm=60,
                use_cache=True
            )
            return result
        except Exception as e:
            logger.error(f"Generation job failed: {e}")
            raise
    
    # Start job in background
    start_job(
        job_id,
        generation_job,
        db,
        data_source_id,
        [t.id for t in tables],
        params
    )
    
    estimated_time = len(tables) * 2  # 2 seconds per table estimate
    
    return GenerationResponse(
        job_id=job_id,
        tables_queued=len(tables),
        estimated_time_seconds=estimated_time
    )


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """Get status of a generation job."""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return JobStatusResponse(
        job_id=job.id,
        status=job.status,
        progress=job.progress.percentage / 100.0,
        items_completed=job.progress.items_processed,
        items_total=job.progress.total_items,
        errors=[job.error] if job.error else [],
        started_at=job.started_at,
        completed_at=job.completed_at
    )


@router.get("/jobs", response_model=List[JobStatusResponse])
async def list_all_jobs(
    job_type: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50
):
    """List all jobs with optional filtering."""
    job_type_enum = JobType(job_type) if job_type else None
    status_enum = None
    if status:
        from ..services.job_manager import JobStatus
        status_enum = JobStatus(status)
    
    jobs = list_jobs(job_type=job_type_enum, status=status_enum, limit=limit)
    
    return [
        JobStatusResponse(
            job_id=job.id,
            status=job.status,
            progress=job.progress.percentage / 100.0,
            items_completed=job.progress.items_processed,
            items_total=job.progress.total_items,
            errors=[job.error] if job.error else [],
            started_at=job.started_at,
            completed_at=job.completed_at
        )
        for job in jobs
    ]


# ERD endpoint
@router.post("/erd", response_model=ERDResponse)
async def generate_erd(
    request: ERDRequest,
    db: Session = Depends(get_db)
):
    """Generate ERD diagram for a data source."""
    # Get tables
    query = db.query(Table).filter(Table.data_source_id == request.data_source_id)
    
    if request.schema_filter:
        query = query.filter(Table.schema_name == request.schema_filter)
    
    # Only included tables
    query = query.outerjoin(TableFilter).filter(
        or_(
            TableFilter.is_included == True,
            TableFilter.id == None
        )
    )
    
    tables = query.limit(request.max_tables).all()
    
    # Get relationships
    table_ids = [t.id for t in tables]
    relationships = db.query(Relationship).filter(
        and_(
            Relationship.source_table_id.in_(table_ids),
            Relationship.target_table_id.in_(table_ids)
        )
    ).all()
    
    # Generate Mermaid diagram
    mermaid = "erDiagram\n"
    
    for table in tables:
        mermaid += f"    {table.table_name} {{\n"
        
        if request.include_columns:
            columns = db.query(Column).filter(Column.table_id == table.id).limit(10).all()
            for col in columns:
                type_str = col.data_type.upper()[:10]  # Truncate long types
                nullable = "NULL" if col.is_nullable else "NOT_NULL"
                mermaid += f"        {type_str} {col.column_name} {nullable}\n"
        
        mermaid += "    }\n\n"
    
    # Add relationships
    for rel in relationships:
        source_table = next((t for t in tables if t.id == rel.source_table_id), None)
        target_table = next((t for t in tables if t.id == rel.target_table_id), None)
        
        if source_table and target_table:
            rel_symbol = {
                'one_to_one': '||--||',
                'one_to_many': '||--o{',
                'many_to_many': '}o--o{'
            }.get(rel.relationship_type, '||--||')
            
            mermaid += f"    {source_table.table_name} {rel_symbol} {target_table.table_name} : \"{rel.relationship_type}\"\n"
    
    return ERDResponse(
        diagram=mermaid,
        table_count=len(tables),
        relationship_count=len(relationships),
        format="mermaid"
    )