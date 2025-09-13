"""Core catalog models for tables, columns, and relationships."""

from sqlalchemy import Column as SQLColumn, Integer, String, DateTime, Float, Boolean, Text, ForeignKey, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base


class DataSource(Base):
    """Represents a connected data source (database)."""
    
    __tablename__ = "data_sources"
    
    id = SQLColumn(Integer, primary_key=True, index=True)
    name = SQLColumn(String(255), unique=True, index=True, nullable=False)
    connection_string = SQLColumn(String(500), nullable=False)
    database_type = SQLColumn(String(50), nullable=False)  # postgresql, mysql, etc.
    description = SQLColumn(Text)
    is_active = SQLColumn(Boolean, default=True)
    
    # Multi-database/schema support
    databases = SQLColumn(JSON)  # List of databases to include (for servers with multiple DBs)
    included_schemas = SQLColumn(JSON)  # List of schemas to include
    excluded_schemas = SQLColumn(JSON)  # List of schemas to exclude
    included_tables_pattern = SQLColumn(String(500))  # Regex pattern for tables to include
    excluded_tables_pattern = SQLColumn(String(500))  # Regex pattern for tables to exclude
    
    # Processing preferences
    auto_profile = SQLColumn(Boolean, default=True)  # Auto-profile on discovery
    sample_size = SQLColumn(Integer, default=10000)  # Sample size for profiling large tables
    
    created_at = SQLColumn(DateTime(timezone=True), server_default=func.now())
    updated_at = SQLColumn(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    tables = relationship("Table", back_populates="data_source", cascade="all, delete-orphan")
    table_filters = relationship("TableFilter", back_populates="data_source", cascade="all, delete-orphan")


class Table(Base):
    """Represents a database table."""
    
    __tablename__ = "tables"
    
    id = SQLColumn(Integer, primary_key=True, index=True)
    data_source_id = SQLColumn(Integer, ForeignKey("data_sources.id"), nullable=False)
    schema_name = SQLColumn(String(255), nullable=False)
    table_name = SQLColumn(String(255), nullable=False, index=True)
    table_type = SQLColumn(String(50))  # table, view, materialized_view
    
    # Profiling data
    row_count = SQLColumn(Integer)
    size_bytes = SQLColumn(Integer)
    
    # Metadata
    created_at = SQLColumn(DateTime(timezone=True), server_default=func.now())
    updated_at = SQLColumn(DateTime(timezone=True), onupdate=func.now())
    last_profiled_at = SQLColumn(DateTime(timezone=True))
    
    # Relationships
    data_source = relationship("DataSource", back_populates="tables")
    columns = relationship("Column", back_populates="table", cascade="all, delete-orphan")
    ai_descriptions = relationship("AIDescription", back_populates="table", cascade="all, delete-orphan")
    
    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema_name}.{self.table_name}"


class Column(Base):
    """Represents a database column."""
    
    __tablename__ = "columns"
    
    id = SQLColumn(Integer, primary_key=True, index=True)
    table_id = SQLColumn(Integer, ForeignKey("tables.id"), nullable=False)
    column_name = SQLColumn(String(255), nullable=False, index=True)
    data_type = SQLColumn(String(100), nullable=False)
    is_nullable = SQLColumn(Boolean, nullable=False)
    column_default = SQLColumn(String(255))
    character_maximum_length = SQLColumn(Integer)
    numeric_precision = SQLColumn(Integer)
    numeric_scale = SQLColumn(Integer)
    ordinal_position = SQLColumn(Integer)
    
    # Profiling data
    cardinality = SQLColumn(Integer)  # Number of distinct values
    null_percentage = SQLColumn(Float)
    top_values = SQLColumn(JSON)  # Top N values with counts
    min_value = SQLColumn(String(255))
    max_value = SQLColumn(String(255))
    avg_value = SQLColumn(Float)
    std_dev = SQLColumn(Float)
    
    # Classification flags
    is_pii = SQLColumn(Boolean, default=False)
    is_key = SQLColumn(Boolean, default=False)
    business_domain = SQLColumn(String(100))
    
    # Metadata
    created_at = SQLColumn(DateTime(timezone=True), server_default=func.now())
    updated_at = SQLColumn(DateTime(timezone=True), onupdate=func.now())
    last_profiled_at = SQLColumn(DateTime(timezone=True))
    
    # Relationships
    table = relationship("Table", back_populates="columns")
    ai_descriptions = relationship("AIDescription", back_populates="column", cascade="all, delete-orphan")


class Relationship(Base):
    """Represents inferred or explicit relationships between tables."""
    
    __tablename__ = "relationships"
    
    id = SQLColumn(Integer, primary_key=True, index=True)
    source_table_id = SQLColumn(Integer, ForeignKey("tables.id"), nullable=False)
    source_column_id = SQLColumn(Integer, ForeignKey("columns.id"), nullable=False)
    target_table_id = SQLColumn(Integer, ForeignKey("tables.id"), nullable=False) 
    target_column_id = SQLColumn(Integer, ForeignKey("columns.id"), nullable=False)
    
    relationship_type = SQLColumn(String(50), nullable=False)  # one_to_many, many_to_many, etc.
    confidence_score = SQLColumn(Float)  # AI confidence in this relationship
    is_validated = SQLColumn(Boolean, default=False)  # Human validated
    
    # Evidence for the relationship
    heuristic_score = SQLColumn(Float)  # Based on naming, types, etc.
    semantic_score = SQLColumn(Float)   # LLM semantic assessment
    empirical_score = SQLColumn(Float)  # Join success rate
    
    created_at = SQLColumn(DateTime(timezone=True), server_default=func.now())
    validated_at = SQLColumn(DateTime(timezone=True))
    validated_by = SQLColumn(String(255))  # User who validated