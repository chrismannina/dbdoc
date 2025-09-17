"""Database connector for extracting metadata from PostgreSQL databases."""

import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TableMetadata:
    """Metadata for a database table."""
    schema_name: str
    table_name: str
    table_type: str
    row_count: Optional[int] = None


@dataclass 
class ColumnMetadata:
    """Metadata for a database column."""
    table_schema: str
    table_name: str
    column_name: str
    data_type: str
    is_nullable: bool
    column_default: Optional[str]
    character_maximum_length: Optional[int]
    numeric_precision: Optional[int]
    numeric_scale: Optional[int]
    ordinal_position: int


class DatabaseConnector:
    """Connects to PostgreSQL databases and extracts metadata."""
    
    def __init__(self, connection_string: str):
        """Initialize with database connection string."""
        self.connection_string = connection_string
        self.engine: Optional[Engine] = None
        
    def connect(self) -> bool:
        """Establish database connection."""
        try:
            self.engine = create_engine(self.connection_string)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Successfully connected to database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def get_tables(self, schemas: List[str] = None) -> List[TableMetadata]:
        """Get list of tables from specified schemas."""
        if not self.engine:
            raise RuntimeError("Database not connected")
            
        schema_filter = ""
        if schemas:
            schema_list = "', '".join(schemas)
            schema_filter = f"AND table_schema IN ('{schema_list}')"
            
        query = text(f"""
            SELECT 
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables 
            WHERE table_type IN ('BASE TABLE', 'VIEW')
            {schema_filter}
            ORDER BY table_schema, table_name
        """)
        
        tables = []
        with self.engine.connect() as conn:
            result = conn.execute(query)
            for row in result:
                tables.append(TableMetadata(
                    schema_name=row.table_schema,
                    table_name=row.table_name,
                    table_type=row.table_type
                ))
                
        # Get row counts for tables
        for table in tables:
            try:
                count_query = text(f'''
                    SELECT COUNT(*) as row_count 
                    FROM "{table.schema_name}"."{table.table_name}"
                ''')
                with self.engine.connect() as conn:
                    result = conn.execute(count_query)
                    table.row_count = result.scalar()
            except Exception as e:
                logger.warning(f"Could not get row count for {table.schema_name}.{table.table_name}: {e}")
                table.row_count = None
                
        return tables
    
    def get_columns(self, schema_name: str, table_name: str) -> List[ColumnMetadata]:
        """Get column metadata for a specific table."""
        if not self.engine:
            raise RuntimeError("Database not connected")
            
        query = text("""
            SELECT 
                table_schema,
                table_name,
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = :schema_name 
            AND table_name = :table_name
            ORDER BY ordinal_position
        """)
        
        columns = []
        with self.engine.connect() as conn:
            result = conn.execute(query, {
                "schema_name": schema_name,
                "table_name": table_name
            })
            
            for row in result:
                columns.append(ColumnMetadata(
                    table_schema=row.table_schema,
                    table_name=row.table_name,
                    column_name=row.column_name,
                    data_type=row.data_type,
                    is_nullable=row.is_nullable == 'YES',
                    column_default=row.column_default,
                    character_maximum_length=row.character_maximum_length,
                    numeric_precision=row.numeric_precision,
                    numeric_scale=row.numeric_scale,
                    ordinal_position=row.ordinal_position
                ))
                
        return columns
    
    def sample_column_data(self, schema_name: str, table_name: str, 
                          column_name: str, limit: int = 100) -> List[Any]:
        """Sample data from a specific column."""
        if not self.engine:
            raise RuntimeError("Database not connected")
            
        query = text(f'''
            SELECT DISTINCT "{column_name}"
            FROM "{schema_name}"."{table_name}"
            WHERE "{column_name}" IS NOT NULL
            ORDER BY "{column_name}"
            LIMIT :limit
        ''')
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {"limit": limit})
            return [row[0] for row in result]
    
    def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Execute a custom query and return results."""
        if not self.engine:
            raise RuntimeError("Database not connected")
            
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return [dict(row._mapping) for row in result]