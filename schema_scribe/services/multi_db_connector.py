"""Multi-database connector supporting PostgreSQL, SQLite, and SQL Server."""

import logging
from typing import List, Dict, Any, Optional, Union
from sqlalchemy import create_engine, text, MetaData, inspect
from sqlalchemy.engine import Engine
from dataclasses import dataclass
from enum import Enum
import re

logger = logging.getLogger(__name__)


class DatabaseType(Enum):
    """Supported database types."""
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"
    MSSQL = "mssql"


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


class MultiDatabaseConnector:
    """Connects to multiple database types and extracts metadata."""
    
    def __init__(self, connection_string: str, database_type: DatabaseType):
        """Initialize with database connection string and type."""
        self.connection_string = connection_string
        self.database_type = database_type
        self.engine: Optional[Engine] = None
        
    def connect(self) -> bool:
        """Establish database connection."""
        try:
            self.engine = create_engine(self.connection_string)
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Successfully connected to {self.database_type.value} database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def get_tables(self, schemas: List[str] = None) -> List[TableMetadata]:
        """Get list of tables from specified schemas."""
        if not self.engine:
            raise RuntimeError("Database not connected")
            
        if self.database_type == DatabaseType.POSTGRESQL:
            return self._get_postgresql_tables(schemas)
        elif self.database_type == DatabaseType.SQLITE:
            return self._get_sqlite_tables()
        elif self.database_type == DatabaseType.MSSQL:
            return self._get_mssql_tables(schemas)
        else:
            raise ValueError(f"Unsupported database type: {self.database_type}")
    
    def _get_postgresql_tables(self, schemas: List[str] = None) -> List[TableMetadata]:
        """Get tables from PostgreSQL."""
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
                
        # Get row counts
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
    
    def _get_sqlite_tables(self) -> List[TableMetadata]:
        """Get tables from SQLite."""
        query = text("""
            SELECT 
                name as table_name,
                type as table_type
            FROM sqlite_master 
            WHERE type IN ('table', 'view')
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        
        tables = []
        with self.engine.connect() as conn:
            result = conn.execute(query)
            for row in result:
                tables.append(TableMetadata(
                    schema_name="main",  # SQLite default schema
                    table_name=row.table_name,
                    table_type="BASE TABLE" if row.table_type == "table" else "VIEW"
                ))
        
        # Get row counts
        for table in tables:
            try:
                count_query = text(f'SELECT COUNT(*) as row_count FROM "{table.table_name}"')
                with self.engine.connect() as conn:
                    result = conn.execute(count_query)
                    table.row_count = result.scalar()
            except Exception as e:
                logger.warning(f"Could not get row count for {table.table_name}: {e}")
                table.row_count = None
                
        return tables
    
    def _get_mssql_tables(self, schemas: List[str] = None) -> List[TableMetadata]:
        """Get tables from SQL Server."""
        schema_filter = ""
        if schemas:
            schema_list = "', '".join(schemas)
            schema_filter = f"AND TABLE_SCHEMA IN ('{schema_list}')"
            
        query = text(f"""
            SELECT 
                TABLE_SCHEMA as table_schema,
                TABLE_NAME as table_name,
                TABLE_TYPE as table_type
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
            {schema_filter}
            ORDER BY TABLE_SCHEMA, TABLE_NAME
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
        
        # Get row counts
        for table in tables:
            try:
                count_query = text(f'''
                    SELECT COUNT(*) as row_count 
                    FROM [{table.schema_name}].[{table.table_name}]
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
            
        if self.database_type == DatabaseType.POSTGRESQL:
            return self._get_postgresql_columns(schema_name, table_name)
        elif self.database_type == DatabaseType.SQLITE:
            return self._get_sqlite_columns(table_name)
        elif self.database_type == DatabaseType.MSSQL:
            return self._get_mssql_columns(schema_name, table_name)
        else:
            raise ValueError(f"Unsupported database type: {self.database_type}")
    
    def _get_postgresql_columns(self, schema_name: str, table_name: str) -> List[ColumnMetadata]:
        """Get columns from PostgreSQL."""
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
    
    def _get_sqlite_columns(self, table_name: str) -> List[ColumnMetadata]:
        """Get columns from SQLite."""
        query = text(f"PRAGMA table_info([{table_name}])")
        
        columns = []
        with self.engine.connect() as conn:
            result = conn.execute(query)
            
            for row in result:
                # SQLite PRAGMA returns: cid, name, type, notnull, dflt_value, pk
                columns.append(ColumnMetadata(
                    table_schema="main",
                    table_name=table_name,
                    column_name=row.name,
                    data_type=row.type,
                    is_nullable=not bool(row.notnull),
                    column_default=row.dflt_value,
                    character_maximum_length=None,
                    numeric_precision=None,
                    numeric_scale=None,
                    ordinal_position=row.cid + 1
                ))
                
        return columns
    
    def _get_mssql_columns(self, schema_name: str, table_name: str) -> List[ColumnMetadata]:
        """Get columns from SQL Server."""
        query = text("""
            SELECT 
                TABLE_SCHEMA as table_schema,
                TABLE_NAME as table_name,
                COLUMN_NAME as column_name,
                DATA_TYPE as data_type,
                IS_NULLABLE as is_nullable,
                COLUMN_DEFAULT as column_default,
                CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
                NUMERIC_PRECISION as numeric_precision,
                NUMERIC_SCALE as numeric_scale,
                ORDINAL_POSITION as ordinal_position
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :schema_name 
            AND TABLE_NAME = :table_name
            ORDER BY ORDINAL_POSITION
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
            
        if self.database_type == DatabaseType.POSTGRESQL:
            query = text(f'''
                SELECT DISTINCT "{column_name}"
                FROM "{schema_name}"."{table_name}"
                WHERE "{column_name}" IS NOT NULL
                ORDER BY "{column_name}"
                LIMIT :limit
            ''')
        elif self.database_type == DatabaseType.SQLITE:
            query = text(f'''
                SELECT DISTINCT [{column_name}]
                FROM [{table_name}]
                WHERE [{column_name}] IS NOT NULL
                ORDER BY [{column_name}]
                LIMIT :limit
            ''')
        elif self.database_type == DatabaseType.MSSQL:
            query = text(f'''
                SELECT DISTINCT TOP (:limit) [{column_name}]
                FROM [{schema_name}].[{table_name}]
                WHERE [{column_name}] IS NOT NULL
                ORDER BY [{column_name}]
            ''')
        else:
            raise ValueError(f"Unsupported database type: {self.database_type}")
        
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
    
    @classmethod
    def validate_connection_string(cls, connection_string: str, database_type: DatabaseType) -> bool:
        """Validate connection string format for the given database type."""
        patterns = {
            DatabaseType.POSTGRESQL: r'^postgresql://.*',
            DatabaseType.SQLITE: r'^sqlite:///',
            DatabaseType.MSSQL: r'^mssql\+pyodbc://.*'
        }
        
        pattern = patterns.get(database_type)
        if not pattern:
            return False
            
        return bool(re.match(pattern, connection_string))
    
    @classmethod
    def get_connection_string_example(cls, database_type: DatabaseType) -> str:
        """Get example connection string for the given database type."""
        examples = {
            DatabaseType.POSTGRESQL: "postgresql://user:password@localhost:5432/database",
            DatabaseType.SQLITE: "sqlite:///path/to/database.db",
            DatabaseType.MSSQL: "mssql+pyodbc://user:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
        }
        
        return examples.get(database_type, "")