"""Pydantic schemas for API requests and responses."""

from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

class DatabaseTypeEnum(str, Enum):
    """Supported database types."""
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite" 
    MSSQL = "mssql"


class DataSourceCreate(BaseModel):
    """Schema for creating a new data source."""
    name: str = Field(..., description="Name of the data source")
    connection_string: str = Field(..., description="Database connection string")
    database_type: DatabaseTypeEnum = Field(default=DatabaseTypeEnum.POSTGRESQL, description="Type of database")
    description: Optional[str] = Field(None, description="Description of the data source")
    


class DataSourceResponse(BaseModel):
    """Schema for data source response."""
    id: int
    name: str
    database_type: str
    description: Optional[str]
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class DiscoveryRequest(BaseModel):
    """Schema for schema discovery request."""
    schemas: Optional[List[str]] = Field(
        None, 
        description="List of schema names to discover. If None, discovers all schemas."
    )


class DiscoveryResponse(BaseModel):
    """Schema for schema discovery response."""
    tables_added: int
    columns_added: int


class GenerateDescriptionsRequest(BaseModel):
    """Schema for generating AI descriptions request."""
    table_ids: Optional[List[int]] = Field(
        None, 
        description="List of table IDs to generate descriptions for. If None, generates for all tables."
    )
    column_ids: Optional[List[int]] = Field(
        None, 
        description="List of column IDs to generate descriptions for. If None, generates for all columns."
    )


class TableResponse(BaseModel):
    """Schema for table list response."""
    id: int
    schema_name: str
    table_name: str
    full_name: str
    table_type: str
    row_count: Optional[int]
    column_count: int
    description: Optional[str]
    ai_confidence: Optional[float]
    last_profiled_at: Optional[datetime]


class ColumnResponse(BaseModel):
    """Schema for column response."""
    id: int
    name: str
    data_type: str
    is_nullable: bool
    is_pii: bool
    is_key: bool
    cardinality: Optional[int]
    null_percentage: Optional[float]
    description: Optional[str]
    ai_confidence: Optional[float]
    validation_status: str


class TableDetailResponse(BaseModel):
    """Schema for detailed table response."""
    id: int
    data_source_id: int
    schema_name: str
    table_name: str
    full_name: str
    table_type: str
    row_count: Optional[int]
    description: Optional[str]
    ai_confidence: Optional[float]
    validation_status: str
    columns: List[ColumnResponse]


class ValidationRequest(BaseModel):
    """Schema for validating AI descriptions."""
    action: str = Field(..., description="Action: 'approve', 'edit', or 'reject'")
    feedback: Optional[str] = Field(None, description="Human feedback or corrected description")


class ValidationResponse(BaseModel):
    """Schema for validation response."""
    success: bool
    message: str