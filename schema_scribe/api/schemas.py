"""Pydantic schemas for API requests and responses."""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class DatabaseType(str, Enum):
    """Supported database types."""
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"
    MSSQL = "mssql"


class Priority(str, Enum):
    """Priority levels for tables."""
    CRITICAL = "critical"
    IMPORTANT = "important"
    NORMAL = "normal"
    LOW = "low"
    IGNORE = "ignore"


class ConfidenceLevel(str, Enum):
    """Confidence levels for user context."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNCERTAIN = "uncertain"


class ContextType(str, Enum):
    """Types of user context."""
    COMPLETE = "complete"
    PARTIAL = "partial"
    HINTS_ONLY = "hints_only"


# DataSource Schemas
class DataSourceCreate(BaseModel):
    """Schema for creating a data source."""
    name: str
    connection_string: str
    database_type: DatabaseType
    description: Optional[str] = None
    databases: Optional[List[str]] = None
    included_schemas: Optional[List[str]] = None
    excluded_schemas: Optional[List[str]] = None
    included_tables_pattern: Optional[str] = None
    excluded_tables_pattern: Optional[str] = None
    auto_profile: bool = True
    sample_size: int = 10000


class DataSourceUpdate(BaseModel):
    """Schema for updating a data source."""
    name: Optional[str] = None
    description: Optional[str] = None
    databases: Optional[List[str]] = None
    included_schemas: Optional[List[str]] = None
    excluded_schemas: Optional[List[str]] = None
    included_tables_pattern: Optional[str] = None
    excluded_tables_pattern: Optional[str] = None
    auto_profile: Optional[bool] = None
    sample_size: Optional[int] = None
    is_active: Optional[bool] = None


class DataSourceResponse(BaseModel):
    """Schema for data source response."""
    id: int
    name: str
    database_type: str
    description: Optional[str]
    is_active: bool
    databases: Optional[List[str]]
    included_schemas: Optional[List[str]]
    excluded_schemas: Optional[List[str]]
    included_tables_pattern: Optional[str]
    excluded_tables_pattern: Optional[str]
    auto_profile: bool
    sample_size: int
    created_at: datetime
    updated_at: Optional[datetime]
    table_count: Optional[int] = 0
    
    class Config:
        from_attributes = True


# Table Schemas
class TableResponse(BaseModel):
    """Schema for table response."""
    id: int
    data_source_id: int
    schema_name: str
    table_name: str
    table_type: Optional[str]
    row_count: Optional[int]
    size_bytes: Optional[int]
    created_at: datetime
    updated_at: Optional[datetime]
    last_profiled_at: Optional[datetime]
    column_count: Optional[int] = 0
    has_description: bool = False
    is_included: bool = True
    priority: Optional[str] = "normal"
    
    class Config:
        from_attributes = True


class TableListParams(BaseModel):
    """Parameters for listing tables."""
    data_source_id: int
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)
    search: Optional[str] = None
    schema_filter: Optional[str] = None
    has_descriptions: Optional[bool] = None
    is_included: Optional[bool] = None
    priority: Optional[Priority] = None


class PaginatedTableResponse(BaseModel):
    """Paginated response for tables."""
    items: List[TableResponse]
    total: int
    limit: int
    offset: int
    has_more: bool


# TableFilter Schemas
class TableFilterCreate(BaseModel):
    """Schema for creating a table filter."""
    table_id: int
    is_included: bool = True
    priority: Priority = Priority.NORMAL
    reason: Optional[str] = None


class TableFilterBulkUpdate(BaseModel):
    """Schema for bulk updating table filters."""
    table_ids: List[int]
    is_included: Optional[bool] = None
    priority: Optional[Priority] = None
    reason: Optional[str] = None


class TableFilterResponse(BaseModel):
    """Schema for table filter response."""
    id: int
    data_source_id: int
    table_id: int
    is_included: bool
    priority: str
    reason: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]
    updated_by: Optional[str]
    
    class Config:
        from_attributes = True


# UserContext Schemas
class UserContextCreate(BaseModel):
    """Schema for creating user context."""
    table_id: Optional[int] = None
    column_id: Optional[int] = None
    business_description: Optional[str] = None
    business_purpose: Optional[str] = None
    data_sources: Optional[str] = None
    data_consumers: Optional[str] = None
    business_rules: Optional[List[str]] = None
    examples: Optional[List[Dict[str, Any]]] = None
    glossary: Optional[Dict[str, str]] = None
    notes: Optional[str] = None
    confidence_level: Optional[ConfidenceLevel] = None
    context_type: Optional[ContextType] = None


class UserContextUpdate(BaseModel):
    """Schema for updating user context."""
    business_description: Optional[str] = None
    business_purpose: Optional[str] = None
    data_sources: Optional[str] = None
    data_consumers: Optional[str] = None
    business_rules: Optional[List[str]] = None
    examples: Optional[List[Dict[str, Any]]] = None
    glossary: Optional[Dict[str, str]] = None
    notes: Optional[str] = None
    confidence_level: Optional[ConfidenceLevel] = None
    context_type: Optional[ContextType] = None


class UserContextResponse(BaseModel):
    """Schema for user context response."""
    id: int
    table_id: Optional[int]
    column_id: Optional[int]
    business_description: Optional[str]
    business_purpose: Optional[str]
    data_sources: Optional[str]
    data_consumers: Optional[str]
    business_rules: Optional[List[str]]
    examples: Optional[List[Dict[str, Any]]]
    glossary: Optional[Dict[str, str]]
    notes: Optional[str]
    confidence_level: Optional[str]
    context_type: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]
    created_by: Optional[str]
    updated_by: Optional[str]
    
    class Config:
        from_attributes = True


# Column Schemas
class ColumnResponse(BaseModel):
    """Schema for column response."""
    id: int
    table_id: int
    column_name: str
    data_type: str
    is_nullable: bool
    column_default: Optional[str]
    character_maximum_length: Optional[int]
    numeric_precision: Optional[int]
    numeric_scale: Optional[int]
    ordinal_position: int
    cardinality: Optional[int]
    null_percentage: Optional[float]
    is_pii: bool
    is_key: bool
    business_domain: Optional[str]
    has_description: bool = False
    has_user_context: bool = False
    
    class Config:
        from_attributes = True


# Discovery Schemas
class DiscoveryParams(BaseModel):
    """Parameters for schema discovery."""
    schemas: Optional[List[str]] = None
    sample_tables: bool = False  # If true, only discover a sample of tables
    sample_size: int = 10  # Number of tables to sample
    auto_profile: bool = True


class DiscoveryResponse(BaseModel):
    """Response for schema discovery."""
    tables_discovered: int
    columns_discovered: int
    schemas_processed: List[str]
    duration_seconds: float
    errors: Optional[List[str]] = None


# Generation Schemas
class GenerationParams(BaseModel):
    """Parameters for AI generation."""
    table_ids: Optional[List[int]] = None  # If None, generate for all
    use_user_context: bool = True
    batch_size: int = 10
    include_columns: bool = True


class GenerationResponse(BaseModel):
    """Response for AI generation."""
    job_id: str
    tables_queued: int
    estimated_time_seconds: float


# Job Status Schemas
class JobStatus(str, Enum):
    """Job status types."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobStatusResponse(BaseModel):
    """Response for job status."""
    job_id: str
    status: JobStatus
    progress: float  # 0.0 to 1.0
    items_completed: int
    items_total: int
    errors: Optional[List[str]] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


# ERD Schemas
class ERDRequest(BaseModel):
    """Request for ERD generation."""
    data_source_id: int
    schema_filter: Optional[str] = None
    include_columns: bool = False
    max_tables: int = 50
    layout: str = "auto"  # auto, hierarchical, circular


class ERDResponse(BaseModel):
    """Response for ERD generation."""
    diagram: str  # Mermaid diagram syntax
    table_count: int
    relationship_count: int
    format: str = "mermaid"