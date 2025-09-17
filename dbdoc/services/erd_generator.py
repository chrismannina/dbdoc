"""ERD generation service for visualizing database relationships."""

import logging
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
import re

from ..models import Table, Column, Relationship, TableFilter
from .relationship_detector import RelationshipDetector

logger = logging.getLogger(__name__)


class ERDGenerator:
    """Generate Entity Relationship Diagrams for database schemas."""
    
    def __init__(self, db_session: Session):
        """Initialize ERD generator with database session."""
        self.db = db_session
    
    def generate_mermaid_erd(self,
                           data_source_id: int,
                           schema_filter: Optional[str] = None,
                           include_columns: bool = True,
                           max_columns_per_table: int = 10,
                           max_tables: int = 50,
                           only_included: bool = True) -> str:
        """Generate Mermaid.js ERD diagram."""
        
        # Get tables
        tables = self._get_tables(data_source_id, schema_filter, max_tables, only_included)
        
        # Detect relationships if not already present
        relationships = self._get_or_detect_relationships(tables, data_source_id)
        
        # Build Mermaid diagram
        diagram = self._build_mermaid_diagram(
            tables, relationships, include_columns, max_columns_per_table
        )
        
        return diagram
    
    def detect_relationships(self, data_source_id: int) -> List[Relationship]:
        """Detect relationships between tables based on various heuristics."""
        
        tables = self.db.query(Table).filter(Table.data_source_id == data_source_id).all()
        detected_relationships = []
        
        for source_table in tables:
            source_columns = self.db.query(Column).filter(Column.table_id == source_table.id).all()
            
            for source_column in source_columns:
                # Check for foreign key patterns
                fk_matches = self._detect_foreign_key_pattern(source_column, tables)
                
                for target_table, target_column, confidence in fk_matches:
                    # Check if relationship already exists
                    existing = self.db.query(Relationship).filter(
                        and_(
                            Relationship.source_table_id == source_table.id,
                            Relationship.source_column_id == source_column.id,
                            Relationship.target_table_id == target_table.id,
                            Relationship.target_column_id == target_column.id
                        )
                    ).first()
                    
                    if not existing:
                        rel = Relationship(
                            source_table_id=source_table.id,
                            source_column_id=source_column.id,
                            target_table_id=target_table.id,
                            target_column_id=target_column.id,
                            relationship_type=self._infer_relationship_type(
                                source_column, target_column
                            ),
                            confidence_score=confidence,
                            heuristic_score=confidence,
                            is_validated=False
                        )
                        detected_relationships.append(rel)
                        self.db.add(rel)
        
        if detected_relationships:
            self.db.commit()
            logger.info(f"Detected {len(detected_relationships)} new relationships")
        
        return detected_relationships
    
    def _get_tables(self,
                   data_source_id: int,
                   schema_filter: Optional[str],
                   max_tables: int,
                   only_included: bool) -> List[Table]:
        """Get tables for ERD generation."""
        
        query = self.db.query(Table).filter(Table.data_source_id == data_source_id)
        
        if schema_filter:
            query = query.filter(Table.schema_name == schema_filter)
        
        if only_included:
            # Only get tables that are included (or have no filter)
            query = query.outerjoin(TableFilter).filter(
                or_(
                    TableFilter.is_included == True,
                    TableFilter.id == None
                )
            )
        
        # Prioritize important tables
        query = query.outerjoin(TableFilter).order_by(
            TableFilter.priority.desc().nullsfirst(),
            Table.row_count.desc().nullsfirst()
        )
        
        return query.limit(max_tables).all()
    
    def _get_or_detect_relationships(self, tables: List[Table], data_source_id: int) -> List[Relationship]:
        """Get existing relationships or detect new ones."""
        
        table_ids = [t.id for t in tables]
        
        # Get existing relationships
        relationships = self.db.query(Relationship).filter(
            and_(
                Relationship.source_table_id.in_(table_ids),
                Relationship.target_table_id.in_(table_ids)
            )
        ).all()
        
        # If no relationships exist, try to detect them
        if not relationships and tables:
            logger.info("No existing relationships found, attempting detection...")
            detector = RelationshipDetector(self.db)
            detector.detect_all_relationships(data_source_id)
            
            # Fetch again after detection
            relationships = self.db.query(Relationship).filter(
                and_(
                    Relationship.source_table_id.in_(table_ids),
                    Relationship.target_table_id.in_(table_ids)
                )
            ).all()
        
        return relationships
    
    def _detect_foreign_key_pattern(self,
                                   column: Column,
                                   tables: List[Table]) -> List[Tuple[Table, Column, float]]:
        """Detect foreign key patterns in column names."""
        
        matches = []
        column_name_lower = column.column_name.lower()
        
        # Common FK patterns
        fk_patterns = [
            (r'(.+)_id$', 1.0),           # ends with _id
            (r'(.+)_fk$', 0.9),           # ends with _fk
            (r'fk_(.+)', 0.9),            # starts with fk_
            (r'(.+)_key$', 0.8),          # ends with _key
            (r'(.+)_code$', 0.7),         # ends with _code
            (r'(.+)_number$', 0.6),       # ends with _number
        ]
        
        for pattern, base_confidence in fk_patterns:
            match = re.match(pattern, column_name_lower)
            if match:
                potential_table_name = match.group(1)
                
                # Look for matching tables
                for target_table in tables:
                    if target_table.id == column.table_id:
                        continue  # Skip self-references for now
                    
                    table_name_lower = target_table.table_name.lower()
                    
                    # Check for exact match or singular/plural variations
                    if (potential_table_name == table_name_lower or
                        potential_table_name + 's' == table_name_lower or
                        potential_table_name == table_name_lower + 's' or
                        potential_table_name.rstrip('s') == table_name_lower.rstrip('s')):
                        
                        # Look for primary key in target table
                        target_columns = self.db.query(Column).filter(
                            Column.table_id == target_table.id
                        ).all()
                        
                        for target_column in target_columns:
                            # Check if it's likely a primary key
                            if (target_column.column_name.lower() in ['id', 'pk', target_table.table_name.lower() + '_id'] or
                                target_column.is_key or
                                (target_column.cardinality and 
                                 target_table.row_count and 
                                 target_column.cardinality >= target_table.row_count * 0.95)):
                                
                                # Adjust confidence based on data type match
                                confidence = base_confidence
                                if column.data_type == target_column.data_type:
                                    confidence = min(confidence + 0.1, 1.0)
                                
                                matches.append((target_table, target_column, confidence))
                                break
        
        return matches
    
    def _infer_relationship_type(self, source_column: Column, target_column: Column) -> str:
        """Infer the type of relationship between columns."""
        
        # Simple heuristic based on cardinality
        if source_column.cardinality and target_column.cardinality:
            source_table = self.db.query(Table).filter(Table.id == source_column.table_id).first()
            
            if source_table and source_table.row_count:
                uniqueness_ratio = source_column.cardinality / source_table.row_count
                
                if uniqueness_ratio > 0.95:
                    return "one_to_one"
                else:
                    return "one_to_many"
        
        # Default to one_to_many
        return "one_to_many"
    
    def _build_mermaid_diagram(self,
                              tables: List[Table],
                              relationships: List[Relationship],
                              include_columns: bool,
                              max_columns_per_table: int) -> str:
        """Build Mermaid.js ERD diagram syntax."""
        
        lines = ["erDiagram"]
        lines.append("")
        
        # Add tables and their columns
        for table in tables:
            lines.append(f"    {self._sanitize_name(table.table_name)} {{")
            
            if include_columns:
                columns = self.db.query(Column).filter(
                    Column.table_id == table.id
                ).order_by(
                    Column.is_key.desc(),
                    Column.ordinal_position
                ).limit(max_columns_per_table).all()
                
                for column in columns:
                    # Format column line
                    data_type = self._format_data_type(column.data_type)
                    column_name = self._sanitize_name(column.column_name)
                    
                    # Add constraints
                    constraints = []
                    if column.is_key:
                        constraints.append("PK")
                    if not column.is_nullable:
                        constraints.append("NOT NULL")
                    if column.is_pii:
                        constraints.append("PII")
                    
                    constraint_str = f" \"{','.join(constraints)}\"" if constraints else ""
                    
                    lines.append(f"        {data_type} {column_name}{constraint_str}")
            
            lines.append("    }")
            lines.append("")
        
        # Add relationships
        for rel in relationships:
            source_table = next((t for t in tables if t.id == rel.source_table_id), None)
            target_table = next((t for t in tables if t.id == rel.target_table_id), None)
            
            if source_table and target_table:
                # Determine relationship notation
                rel_notation = self._get_mermaid_notation(rel.relationship_type)
                
                # Add confidence indicator if not validated
                label = ""
                if not rel.is_validated and rel.confidence_score:
                    confidence_pct = int(rel.confidence_score * 100)
                    label = f" : \"confidence: {confidence_pct}%\""
                
                lines.append(
                    f"    {self._sanitize_name(source_table.table_name)} {rel_notation} "
                    f"{self._sanitize_name(target_table.table_name)}{label}"
                )
        
        return "\n".join(lines)
    
    def _sanitize_name(self, name: str) -> str:
        """Sanitize names for Mermaid diagram."""
        # Replace spaces and special characters
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # Ensure it doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = f"t_{sanitized}"
        return sanitized
    
    def _format_data_type(self, data_type: str) -> str:
        """Format data type for display."""
        # Truncate long data types
        if len(data_type) > 15:
            return data_type[:12] + "..."
        return data_type.upper()
    
    def _get_mermaid_notation(self, relationship_type: str) -> str:
        """Get Mermaid notation for relationship type."""
        notations = {
            "one_to_one": "||--||",
            "one_to_many": "||--o{",
            "many_to_one": "}o--||",
            "many_to_many": "}o--o{",
            "zero_to_one": "|o--||",
            "zero_to_many": "|o--o{"
        }
        return notations.get(relationship_type, "||--||")