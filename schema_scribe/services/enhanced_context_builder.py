"""Enhanced context builder for more accurate AI description generation."""

import logging
from typing import List, Dict, Any, Optional, Set
from sqlalchemy.orm import Session
from dataclasses import dataclass
from ..models import DataSource, Table, Column, AIDescription
from ..services.multi_db_connector import MultiDatabaseConnector, DatabaseType

logger = logging.getLogger(__name__)


@dataclass
class RelationshipInfo:
    """Information about table relationships."""
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    constraint_name: str


@dataclass
class SchemaPattern:
    """Detected naming and structural patterns in the schema."""
    table_prefixes: List[str]
    column_suffixes: List[str]
    naming_convention: str
    business_domains: List[str]


@dataclass
class EnhancedContext:
    """Rich context for AI generation."""
    # Basic context
    target_name: str
    target_type: str  # 'table' or 'column'
    basic_metadata: Dict[str, Any]
    
    # Relational context
    relationships: List[RelationshipInfo]
    related_tables: List[str]
    
    # Business context
    schema_patterns: SchemaPattern
    domain_hints: List[str]
    
    # Data quality context
    constraints: List[str]
    indexes: List[str]
    data_quality_issues: List[str]
    
    # Historical context
    similar_descriptions: List[Dict[str, Any]]
    validation_feedback: List[str]


class EnhancedContextBuilder:
    """Builds rich, comprehensive context for AI description generation."""
    
    def __init__(self, db_session: Session, data_source: DataSource):
        self.db = db_session
        self.data_source = data_source
        self.connector = None
        self._schema_patterns_cache = None
        self._relationships_cache = None
        
    def _get_connector(self) -> MultiDatabaseConnector:
        """Get database connector, creating if needed."""
        if self.connector is None:
            db_type = DatabaseType(self.data_source.database_type)
            self.connector = MultiDatabaseConnector(
                self.data_source.connection_string, 
                db_type
            )
            self.connector.connect()
        return self.connector
    
    def _detect_schema_patterns(self) -> SchemaPattern:
        """Analyze schema to detect naming patterns and business domains."""
        if self._schema_patterns_cache:
            return self._schema_patterns_cache
            
        tables = self.db.query(Table).filter(
            Table.data_source_id == self.data_source.id
        ).all()
        
        # Analyze table naming patterns
        table_names = [t.table_name.lower() for t in tables]
        table_prefixes = self._extract_prefixes(table_names)
        
        # Analyze column naming patterns
        all_columns = []
        for table in tables:
            all_columns.extend([c.column_name.lower() for c in table.columns])
        
        column_suffixes = self._extract_suffixes(all_columns)
        
        # Infer business domains
        business_domains = self._infer_business_domains(table_names, all_columns)
        
        # Detect naming convention
        naming_convention = self._detect_naming_convention(table_names + all_columns)
        
        self._schema_patterns_cache = SchemaPattern(
            table_prefixes=table_prefixes,
            column_suffixes=column_suffixes,
            naming_convention=naming_convention,
            business_domains=business_domains
        )
        
        return self._schema_patterns_cache
    
    def _extract_prefixes(self, names: List[str], min_count: int = 2) -> List[str]:
        """Extract common prefixes from a list of names."""
        prefix_counts = {}
        
        for name in names:
            for i in range(2, min(len(name), 8)):  # Check prefixes 2-7 chars
                prefix = name[:i]
                if prefix.endswith('_'):
                    prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1
        
        return [prefix for prefix, count in prefix_counts.items() if count >= min_count]
    
    def _extract_suffixes(self, names: List[str], min_count: int = 3) -> List[str]:
        """Extract common suffixes from a list of names."""
        suffix_counts = {}
        
        common_suffixes = ['_id', '_key', '_date', '_time', '_flag', '_status', '_type', '_code', '_name']
        
        for name in names:
            for suffix in common_suffixes:
                if name.endswith(suffix):
                    suffix_counts[suffix] = suffix_counts.get(suffix, 0) + 1
        
        return [suffix for suffix, count in suffix_counts.items() if count >= min_count]
    
    def _infer_business_domains(self, table_names: List[str], column_names: List[str]) -> List[str]:
        """Infer business domains from naming patterns."""
        domain_keywords = {
            'finance': ['payment', 'invoice', 'billing', 'account', 'transaction', 'price', 'cost', 'revenue'],
            'hr': ['employee', 'staff', 'person', 'user', 'role', 'department', 'salary'],
            'sales': ['order', 'customer', 'product', 'sale', 'deal', 'lead', 'opportunity'],
            'inventory': ['stock', 'inventory', 'warehouse', 'item', 'product', 'supplier'],
            'marketing': ['campaign', 'lead', 'prospect', 'channel', 'conversion', 'analytics'],
            'operations': ['process', 'workflow', 'task', 'status', 'queue', 'log'],
            'analytics': ['metric', 'kpi', 'report', 'dashboard', 'analysis', 'aggregation']
        }
        
        detected_domains = []
        all_names = table_names + column_names
        
        for domain, keywords in domain_keywords.items():
            if any(keyword in name for name in all_names for keyword in keywords):
                detected_domains.append(domain)
        
        return detected_domains
    
    def _detect_naming_convention(self, names: List[str]) -> str:
        """Detect the naming convention used."""
        snake_case_count = sum(1 for name in names if '_' in name and name.islower())
        camel_case_count = sum(1 for name in names if any(c.isupper() for c in name[1:]) and '_' not in name)
        
        total = len(names)
        if snake_case_count / total > 0.7:
            return "snake_case"
        elif camel_case_count / total > 0.7:
            return "camelCase"
        else:
            return "mixed"
    
    def _get_table_relationships(self, table_name: str) -> List[RelationshipInfo]:
        """Get foreign key relationships for a table."""
        if self._relationships_cache is None:
            self._relationships_cache = self._load_all_relationships()
        
        return [rel for rel in self._relationships_cache 
                if rel.source_table == table_name or rel.target_table == table_name]
    
    def _load_all_relationships(self) -> List[RelationshipInfo]:
        """Load all foreign key relationships from the database."""
        connector = self._get_connector()
        relationships = []
        
        try:
            # This is database-specific - would need to implement for each DB type
            if self.data_source.database_type == 'postgresql':
                relationships = self._load_postgresql_relationships()
            elif self.data_source.database_type == 'sqlite':
                relationships = self._load_sqlite_relationships()
            # Add other database types as needed
        except Exception as e:
            logger.warning(f"Failed to load relationships: {e}")
            
        return relationships
    
    def _load_postgresql_relationships(self) -> List[RelationshipInfo]:
        """Load relationships from PostgreSQL system tables."""
        # Implementation would query information_schema.key_column_usage and related tables
        # This is a simplified placeholder
        return []
    
    def _load_sqlite_relationships(self) -> List[RelationshipInfo]:
        """Load relationships from SQLite using PRAGMA foreign_key_list."""
        # Implementation would use SQLite's PRAGMA commands
        # This is a simplified placeholder
        return []
    
    def _get_similar_descriptions(self, target_name: str, target_type: str) -> List[Dict[str, Any]]:
        """Find similar previously generated descriptions for context."""
        if target_type == 'table':
            similar = self.db.query(AIDescription).filter(
                AIDescription.table_id.isnot(None),
                AIDescription.status.in_(['approved', 'edited'])
            ).limit(5).all()
        else:
            similar = self.db.query(AIDescription).filter(
                AIDescription.column_id.isnot(None),
                AIDescription.status.in_(['approved', 'edited'])
            ).limit(5).all()
        
        return [
            {
                'name': desc.final_description or desc.description,
                'confidence': desc.confidence_score,
                'domain': desc.suggested_business_domain
            }
            for desc in similar
        ]
    
    def build_table_context(self, table: Table) -> EnhancedContext:
        """Build comprehensive context for table description generation."""
        # Get schema patterns
        patterns = self._detect_schema_patterns()
        
        # Get relationships
        relationships = self._get_table_relationships(table.table_name)
        related_tables = list(set([
            rel.target_table if rel.source_table == table.table_name else rel.source_table
            for rel in relationships
        ]))
        
        # Basic metadata
        basic_metadata = {
            'schema_name': table.schema_name,
            'table_name': table.table_name,
            'table_type': table.table_type,
            'row_count': table.row_count,
            'column_count': len(table.columns),
            'columns': [
                {
                    'name': col.column_name,
                    'type': col.data_type,
                    'nullable': col.is_nullable,
                    'is_key': col.is_key,
                    'is_pii': col.is_pii
                }
                for col in table.columns
            ]
        }
        
        # Domain hints from patterns
        domain_hints = []
        for domain in patterns.business_domains:
            if any(keyword in table.table_name.lower() 
                   for keyword in self._get_domain_keywords(domain)):
                domain_hints.append(domain)
        
        # Get similar descriptions for examples
        similar_descriptions = self._get_similar_descriptions(table.table_name, 'table')
        
        return EnhancedContext(
            target_name=f"{table.schema_name}.{table.table_name}",
            target_type='table',
            basic_metadata=basic_metadata,
            relationships=relationships,
            related_tables=related_tables,
            schema_patterns=patterns,
            domain_hints=domain_hints,
            constraints=[],  # Would populate from database metadata
            indexes=[],      # Would populate from database metadata
            data_quality_issues=[],  # Would analyze from profiling
            similar_descriptions=similar_descriptions,
            validation_feedback=[]
        )
    
    def build_column_context(self, column: Column, table_description: Optional[str] = None) -> EnhancedContext:
        """Build comprehensive context for column description generation."""
        table = column.table
        patterns = self._detect_schema_patterns()
        
        # Basic metadata
        basic_metadata = {
            'table_name': table.table_name,
            'schema_name': table.schema_name,
            'column_name': column.column_name,
            'data_type': column.data_type,
            'is_nullable': column.is_nullable,
            'is_key': column.is_key,
            'is_pii': column.is_pii,
            'cardinality': column.cardinality,
            'null_percentage': column.null_percentage,
            'top_values': column.top_values,
            'min_value': column.min_value,
            'max_value': column.max_value,
            'table_description': table_description  # Use table description as context!
        }
        
        # Get relationships involving this column
        table_relationships = self._get_table_relationships(table.table_name)
        column_relationships = [
            rel for rel in table_relationships 
            if rel.source_column == column.column_name or rel.target_column == column.column_name
        ]
        
        # Domain hints
        domain_hints = []
        for domain in patterns.business_domains:
            domain_keywords = self._get_domain_keywords(domain)
            if any(keyword in column.column_name.lower() for keyword in domain_keywords):
                domain_hints.append(domain)
        
        # Detect column purpose from naming patterns
        column_purpose_hints = []
        if column.column_name.lower().endswith('_id'):
            column_purpose_hints.append('identifier')
        if column.column_name.lower().endswith('_date') or column.column_name.lower().endswith('_time'):
            column_purpose_hints.append('temporal')
        if column.column_name.lower().endswith('_flag') or column.column_name.lower().endswith('_status'):
            column_purpose_hints.append('status_indicator')
        
        similar_descriptions = self._get_similar_descriptions(column.column_name, 'column')
        
        return EnhancedContext(
            target_name=f"{table.schema_name}.{table.table_name}.{column.column_name}",
            target_type='column',
            basic_metadata=basic_metadata,
            relationships=column_relationships,
            related_tables=[],
            schema_patterns=patterns,
            domain_hints=domain_hints + column_purpose_hints,
            constraints=[],
            indexes=[],
            data_quality_issues=[],
            similar_descriptions=similar_descriptions,
            validation_feedback=[]
        )
    
    def _get_domain_keywords(self, domain: str) -> List[str]:
        """Get keywords associated with a business domain."""
        domain_keywords = {
            'finance': ['payment', 'invoice', 'billing', 'account', 'transaction', 'price', 'cost', 'revenue'],
            'hr': ['employee', 'staff', 'person', 'user', 'role', 'department', 'salary'],
            'sales': ['order', 'customer', 'product', 'sale', 'deal', 'lead', 'opportunity'],
            'inventory': ['stock', 'inventory', 'warehouse', 'item', 'product', 'supplier'],
            'marketing': ['campaign', 'lead', 'prospect', 'channel', 'conversion', 'analytics'],
            'operations': ['process', 'workflow', 'task', 'status', 'queue', 'log'],
            'analytics': ['metric', 'kpi', 'report', 'dashboard', 'analysis', 'aggregation']
        }
        return domain_keywords.get(domain, [])