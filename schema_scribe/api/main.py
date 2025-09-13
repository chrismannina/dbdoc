"""Main FastAPI application."""

from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from typing import List, Optional
import os

from ..models.base import get_db, create_tables
from ..models import DataSource
from ..core import CatalogManager
from .schemas import (
    DataSourceCreate, DataSourceResponse,
    TableResponse, TableDetailResponse,
    DiscoveryRequest, DiscoveryResponse,
    GenerateDescriptionsRequest,
    ValidationRequest, ValidationResponse
)
from .endpoints import router as enhanced_router

# Create tables on startup
create_tables()

app = FastAPI(
    title="Schema Scribe",
    description="An LLM-powered data catalog",
    version="0.1.0"
)

# Include enhanced API routes
app.include_router(enhanced_router)

# Mount static files and templates
static_path = os.path.join(os.path.dirname(__file__), "..", "web", "static")
templates_path = os.path.join(os.path.dirname(__file__), "..", "web", "templates")

if os.path.exists(static_path):
    app.mount("/static", StaticFiles(directory=static_path), name="static")

templates = Jinja2Templates(directory=templates_path)


# Web Routes
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, db: Session = Depends(get_db)):
    """Home page showing data sources."""
    data_sources = db.query(DataSource).all()
    return templates.TemplateResponse("index.html", {
        "request": request,
        "data_sources": data_sources
    })


@app.get("/catalog/{data_source_id}", response_class=HTMLResponse)
async def catalog_view(request: Request, data_source_id: int, db: Session = Depends(get_db)):
    """Catalog view for a specific data source."""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    catalog_manager = CatalogManager(db)
    tables = catalog_manager.get_tables(data_source_id)
    
    return templates.TemplateResponse("catalog.html", {
        "request": request,
        "data_source": data_source,
        "tables": tables
    })


@app.get("/enhanced-catalog/{data_source_id}", response_class=HTMLResponse)
async def enhanced_catalog_view(request: Request, data_source_id: int, db: Session = Depends(get_db)):
    """Enhanced catalog view with filtering, search, and ERD features."""
    data_source = db.query(DataSource).filter(DataSource.id == data_source_id).first()
    if not data_source:
        raise HTTPException(status_code=404, detail="Data source not found")
    
    return templates.TemplateResponse("enhanced_catalog.html", {
        "request": request,
        "data_source": data_source
    })


@app.get("/table/{table_id}", response_class=HTMLResponse)
async def table_view(request: Request, table_id: int, db: Session = Depends(get_db)):
    """Detailed view of a specific table."""
    catalog_manager = CatalogManager(db)
    table_details = catalog_manager.get_table_details(table_id)
    
    if not table_details:
        raise HTTPException(status_code=404, detail="Table not found")
    
    return templates.TemplateResponse("table.html", {
        "request": request,
        "table": table_details
    })


# API Routes
@app.post("/api/data-sources", response_model=DataSourceResponse)
async def create_data_source(data_source: DataSourceCreate, db: Session = Depends(get_db)):
    """Create a new data source."""
    catalog_manager = CatalogManager(db)
    
    try:
        new_data_source = catalog_manager.add_data_source(
            name=data_source.name,
            connection_string=data_source.connection_string,
            database_type=data_source.database_type
        )
        return DataSourceResponse.from_orm(new_data_source)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/data-sources", response_model=List[DataSourceResponse])
async def list_data_sources(db: Session = Depends(get_db)):
    """List all data sources."""
    data_sources = db.query(DataSource).all()
    return [DataSourceResponse.from_orm(ds) for ds in data_sources]


@app.delete("/api/data-sources/{data_source_id}")
async def delete_data_source(data_source_id: int, db: Session = Depends(get_db)):
    """Delete a data source and all its associated data."""
    catalog_manager = CatalogManager(db)
    
    try:
        result = catalog_manager.remove_data_source(data_source_id)
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/api/data-sources/{data_source_id}/discover", response_model=DiscoveryResponse)
async def discover_schema(data_source_id: int, request: DiscoveryRequest, db: Session = Depends(get_db)):
    """Discover schema for a data source."""
    catalog_manager = CatalogManager(db)
    
    try:
        result = catalog_manager.discover_schema(
            data_source_id=data_source_id,
            schemas=request.schemas
        )
        return DiscoveryResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/data-sources/{data_source_id}/generate-descriptions")
async def generate_descriptions(
    data_source_id: int, 
    request: Optional[GenerateDescriptionsRequest] = None,
    table_id: Optional[int] = None,
    enhanced: bool = False,
    max_concurrent: int = 5,
    rate_limit_rpm: int = 60,
    use_cache: bool = True,
    db: Session = Depends(get_db)
):
    """
    Generate AI descriptions for tables and columns.
    
    Parameters:
    - enhanced: Use enhanced generation with better context and concurrency
    - max_concurrent: Max concurrent AI calls (enhanced mode only)
    - rate_limit_rpm: Rate limit in requests per minute (enhanced mode only)
    - use_cache: Enable caching for faster repeated generations (enhanced mode only)
    """
    catalog_manager = CatalogManager(db)
    
    try:
        table_ids = None
        column_ids = None
        
        if request:
            table_ids = request.table_ids
            column_ids = set(request.column_ids) if request.column_ids else None
        
        if enhanced:
            # Use enhanced generation with rich context and concurrency
            result = catalog_manager.generate_descriptions_sync_wrapper(
                data_source_id=data_source_id,
                table_ids=table_ids,
                column_ids=column_ids,
                max_concurrent=max_concurrent,
                rate_limit_rpm=rate_limit_rpm,
                use_cache=use_cache
            )
        else:
            # Use original generation method
            result = catalog_manager.generate_descriptions(
                data_source_id=data_source_id,
                table_id=table_id,
                table_ids=table_ids,
                column_ids=list(column_ids) if column_ids else None
            )
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/data-sources/{data_source_id}/tables", response_model=List[TableResponse])
async def get_tables(data_source_id: int, db: Session = Depends(get_db)):
    """Get all tables for a data source."""
    catalog_manager = CatalogManager(db)
    tables = catalog_manager.get_tables(data_source_id)
    return [TableResponse(**table) for table in tables]


@app.get("/api/tables/{table_id}", response_model=TableDetailResponse)
async def get_table_details(table_id: int, db: Session = Depends(get_db)):
    """Get detailed information about a table."""
    catalog_manager = CatalogManager(db)
    table_details = catalog_manager.get_table_details(table_id)
    
    if not table_details:
        raise HTTPException(status_code=404, detail="Table not found")
    
    return TableDetailResponse(**table_details)


@app.post("/api/descriptions/{description_id}/validate", response_model=ValidationResponse)
async def validate_description(description_id: int, request: ValidationRequest, db: Session = Depends(get_db)):
    """Validate an AI-generated description."""
    catalog_manager = CatalogManager(db)
    
    try:
        result = catalog_manager.validate_description(
            description_id=description_id,
            action=request.action,
            feedback=request.feedback
        )
        return ValidationResponse(success=True, message="Validation completed successfully")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/descriptions/pending")
async def get_pending_descriptions(db: Session = Depends(get_db)):
    """Get all descriptions pending validation."""
    catalog_manager = CatalogManager(db)
    pending = catalog_manager.get_pending_descriptions()
    return pending