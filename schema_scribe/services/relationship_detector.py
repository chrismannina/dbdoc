"""Advanced relationship detection service for inferring table relationships."""

import logging
import re
from typing import List, Dict, Any, Optional, Tuple, Set
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
from dataclasses import dataclass
from collections import defaultdict, Counter

from ..models import Table, Column, Relationship, DataSource

logger = logging.getLogger(__name__)


@dataclass
class RelationshipCandidate:
    """Candidate relationship between two columns."""
    source_table: Table
    source_column: Column
    target_table: Table
    target_column: Column
    relationship_type: str
    confidence_score: float
    evidence: Dict[str, Any]
    

class RelationshipDetector:
    """Advanced service for detecting relationships between database tables."""
    
    def __init__(self, db_session: Session):
        """Initialize with database session."""
        self.db = db_session
        
    def detect_all_relationships(self, data_source_id: int) -> List[Relationship]:
        """Detect all relationships for a data source."""
        logger.info(f"Starting relationship detection for data source {data_source_id}")
        
        # Get all tables and columns
        tables = self._get_tables_with_columns(data_source_id)
        if len(tables) < 2:
            logger.info("Need at least 2 tables to detect relationships")
            return []
        
        # Find relationship candidates using multiple methods
        candidates = []
        
        # Method 1: Foreign key constraints (explicit)
        candidates.extend(self._detect_foreign_key_relationships(tables))
        
        # Method 2: Naming pattern analysis
        candidates.extend(self._detect_naming_pattern_relationships(tables))
        
        # Method 3: Data distribution analysis
        candidates.extend(self._detect_statistical_relationships(tables))
        
        # Method 4: Column type and cardinality analysis
        candidates.extend(self._detect_structural_relationships(tables))
        
        # Deduplicate and rank candidates
        final_relationships = self._rank_and_deduplicate_candidates(candidates)
        
        # Save to database
        saved_relationships = self._save_relationships(final_relationships)
        
        logger.info(f"Detected {len(saved_relationships)} relationships")
        return saved_relationships
    
    def _get_tables_with_columns(self, data_source_id: int) -> List[Table]:
        """Get all tables with their columns for analysis."""
        return self.db.query(Table).filter(
            Table.data_source_id == data_source_id
        ).all()
    
    def _detect_foreign_key_relationships(self, tables: List[Table]) -> List[RelationshipCandidate]:
        """Detect explicit foreign key relationships."""
        candidates = []
        
        # This would require database-specific introspection
        # For now, we'll focus on naming patterns and data analysis
        logger.debug("Foreign key detection not implemented yet")
        
        return candidates
    
    def _detect_naming_pattern_relationships(self, tables: List[Table]) -> List[RelationshipCandidate]:
        """Detect relationships based on naming patterns."""
        candidates = []
        
        # Build lookup maps
        table_by_name = {t.table_name.lower(): t for t in tables}
        columns_by_table = {t.id: t.columns for t in tables}
        
        # Common FK naming patterns
        fk_patterns = [
            (r'^(.+)_id$', 1.0, 'exact_match'),           # product_id -> product.id
            (r'^(.+)_fk$', 0.9, 'fk_suffix'),             # product_fk -> product.id
            (r'^fk_(.+)$', 0.9, 'fk_prefix'),             # fk_product -> product.id
            (r'^(.+)_key$', 0.8, 'key_suffix'),           # product_key -> product.id
            (r'^(.+)_code$', 0.7, 'code_suffix'),         # product_code -> product.code
            (r'^(.+)_number$', 0.6, 'number_suffix'),     # order_number -> order.number
            (r'^(.+)_ref$', 0.8, 'ref_suffix'),           # customer_ref -> customer.id
            (r'^ref_(.+)$', 0.8, 'ref_prefix'),           # ref_customer -> customer.id
        ]
        
        for source_table in tables:
            for source_column in source_table.columns:
                source_col_lower = source_column.column_name.lower()
                
                # Skip if it's a likely primary key
                if self._is_likely_primary_key(source_column):
                    continue
                
                for pattern, base_confidence, pattern_type in fk_patterns:
                    match = re.match(pattern, source_col_lower)
                    if match:
                        potential_table_name = match.group(1)
                        
                        # Look for target table (handle plural/singular variations)
                        target_candidates = self._find_table_candidates(potential_table_name, table_by_name)
                        
                        for target_table_name, name_confidence in target_candidates:
                            target_table = table_by_name[target_table_name]
                            
                            # Find potential target columns (PK, unique keys, etc.)
                            target_columns = self._find_target_columns(target_table, pattern_type)
                            
                            for target_column, col_confidence in target_columns:
                                # Calculate overall confidence
                                confidence = base_confidence * name_confidence * col_confidence
                                
                                # Type compatibility check
                                type_compatibility = self._check_type_compatibility(
                                    source_column, target_column
                                )
                                confidence *= type_compatibility
                                
                                if confidence > 0.3:  # Threshold for consideration
                                    relationship_type = self._infer_relationship_type(
                                        source_column, target_column, source_table, target_table
                                    )
                                    
                                    candidates.append(RelationshipCandidate(
                                        source_table=source_table,
                                        source_column=source_column,
                                        target_table=target_table,
                                        target_column=target_column,
                                        relationship_type=relationship_type,
                                        confidence_score=confidence,
                                        evidence={
                                            'pattern_type': pattern_type,
                                            'pattern': pattern,
                                            'name_confidence': name_confidence,
                                            'column_confidence': col_confidence,
                                            'type_compatibility': type_compatibility
                                        }
                                    ))
        
        logger.debug(f"Found {len(candidates)} naming pattern candidates")
        return candidates
    
    def _detect_statistical_relationships(self, tables: List[Table]) -> List[RelationshipCandidate]:
        """Detect relationships based on data distribution and cardinality."""
        candidates = []
        
        for source_table in tables:
            for source_column in source_table.columns:
                # Skip if no cardinality data
                if not source_column.cardinality or not source_column.top_values:
                    continue
                
                for target_table in tables:
                    if target_table.id == source_table.id:
                        continue
                    
                    for target_column in target_table.columns:
                        if not target_column.cardinality or not target_column.top_values:
                            continue
                        
                        # Check if there's significant value overlap
                        overlap_score = self._calculate_value_overlap(source_column, target_column)
                        
                        if overlap_score > 0.5:  # Significant overlap
                            # Analyze cardinality patterns
                            relationship_type, confidence = self._analyze_cardinality_pattern(
                                source_column, target_column, source_table, target_table
                            )
                            
                            if confidence > 0.4:
                                candidates.append(RelationshipCandidate(
                                    source_table=source_table,
                                    source_column=source_column,
                                    target_table=target_table,
                                    target_column=target_column,
                                    relationship_type=relationship_type,
                                    confidence_score=confidence,
                                    evidence={
                                        'detection_method': 'statistical',
                                        'value_overlap': overlap_score,
                                        'source_cardinality': source_column.cardinality,
                                        'target_cardinality': target_column.cardinality
                                    }
                                ))
        
        logger.debug(f"Found {len(candidates)} statistical candidates")
        return candidates
    
    def _detect_structural_relationships(self, tables: List[Table]) -> List[RelationshipCandidate]:
        """Detect relationships based on column structure and constraints."""
        candidates = []
        
        # Find columns that look like foreign keys based on structure
        for source_table in tables:
            for source_column in source_table.columns:
                # Look for structural FK indicators
                if self._has_fk_structure(source_column):
                    # Find potential target tables/columns
                    potential_targets = self._find_structural_targets(source_column, tables)
                    
                    for target_table, target_column, confidence in potential_targets:
                        relationship_type = self._infer_relationship_type(
                            source_column, target_column, source_table, target_table
                        )
                        
                        candidates.append(RelationshipCandidate(
                            source_table=source_table,
                            source_column=source_column,
                            target_table=target_table,
                            target_column=target_column,
                            relationship_type=relationship_type,
                            confidence_score=confidence,
                            evidence={
                                'detection_method': 'structural',
                                'fk_indicators': self._get_fk_indicators(source_column)
                            }
                        ))
        
        logger.debug(f"Found {len(candidates)} structural candidates")
        return candidates
    
    def _find_table_candidates(self, table_name: str, table_by_name: Dict[str, Table]) -> List[Tuple[str, float]]:
        """Find table candidates for a given name, handling plural/singular variations."""
        candidates = []
        
        # Exact match
        if table_name in table_by_name:
            candidates.append((table_name, 1.0))
        
        # Plural variations
        plural_forms = [
            table_name + 's',
            table_name + 'es',
            table_name + 'ies' if table_name.endswith('y') else None
        ]
        
        for plural in plural_forms:
            if plural and plural in table_by_name:
                candidates.append((plural, 0.9))
        
        # Singular variations (if input was plural)
        if table_name.endswith('s') and len(table_name) > 1:
            singular = table_name[:-1]
            if singular in table_by_name:
                candidates.append((singular, 0.9))
        
        if table_name.endswith('es') and len(table_name) > 2:
            singular = table_name[:-2]
            if singular in table_by_name:
                candidates.append((singular, 0.9))
        
        if table_name.endswith('ies') and len(table_name) > 3:
            singular = table_name[:-3] + 'y'
            if singular in table_by_name:
                candidates.append((singular, 0.9))
        
        return candidates
    
    def _find_target_columns(self, table: Table, pattern_type: str) -> List[Tuple[Column, float]]:
        """Find potential target columns in a table."""
        candidates = []
        
        for column in table.columns:
            confidence = 0.0
            
            # Primary key gets highest priority
            if self._is_likely_primary_key(column):
                confidence = 1.0
            
            # Unique columns get high priority
            elif column.is_key or (column.cardinality and table.row_count and 
                                 column.cardinality >= table.row_count * 0.95):
                confidence = 0.9
            
            # For specific patterns, look for matching column names
            elif pattern_type == 'code_suffix' and 'code' in column.column_name.lower():
                confidence = 0.8
            elif pattern_type == 'number_suffix' and 'number' in column.column_name.lower():
                confidence = 0.8
            
            # Default for non-nullable columns
            elif not column.is_nullable:
                confidence = 0.5
            
            # Any other column
            else:
                confidence = 0.3
            
            if confidence > 0:
                candidates.append((column, confidence))
        
        return candidates
    
    def _is_likely_primary_key(self, column: Column) -> bool:
        """Check if a column is likely a primary key."""
        col_name_lower = column.column_name.lower()
        
        # Common PK naming patterns
        pk_patterns = ['id', 'pk', f"{column.table.table_name.lower()}_id"]
        
        if col_name_lower in pk_patterns:
            return True
        
        # Check if it's marked as key and has high cardinality
        if (column.is_key and column.cardinality and column.table.row_count and
            column.cardinality >= column.table.row_count * 0.95):
            return True
        
        return False
    
    def _check_type_compatibility(self, source_column: Column, target_column: Column) -> float:
        """Check data type compatibility between columns."""
        source_type = source_column.data_type.lower()
        target_type = target_column.data_type.lower()
        
        # Exact match
        if source_type == target_type:
            return 1.0
        
        # Integer types
        int_types = {'integer', 'int', 'bigint', 'smallint', 'tinyint'}
        if any(t in source_type for t in int_types) and any(t in target_type for t in int_types):
            return 0.9
        
        # String types
        string_types = {'varchar', 'char', 'text', 'string'}
        if any(t in source_type for t in string_types) and any(t in target_type for t in string_types):
            return 0.8
        
        # Numeric types
        numeric_types = {'decimal', 'numeric', 'float', 'double', 'real'}
        if any(t in source_type for t in numeric_types) and any(t in target_type for t in numeric_types):
            return 0.7
        
        # Different types but could work
        return 0.3
    
    def _calculate_value_overlap(self, source_column: Column, target_column: Column) -> float:
        """Calculate the overlap in top values between two columns."""
        if not source_column.top_values or not target_column.top_values:
            return 0.0
        
        source_values = set(source_column.top_values.keys()) if isinstance(source_column.top_values, dict) else set()
        target_values = set(target_column.top_values.keys()) if isinstance(target_column.top_values, dict) else set()
        
        if not source_values or not target_values:
            return 0.0
        
        intersection = source_values.intersection(target_values)
        union = source_values.union(target_values)
        
        return len(intersection) / len(union) if union else 0.0
    
    def _analyze_cardinality_pattern(self, source_column: Column, target_column: Column, 
                                   source_table: Table, target_table: Table) -> Tuple[str, float]:
        """Analyze cardinality patterns to determine relationship type and confidence."""
        
        # Get cardinality ratios
        source_ratio = (source_column.cardinality / source_table.row_count 
                       if source_column.cardinality and source_table.row_count else 0)
        target_ratio = (target_column.cardinality / target_table.row_count 
                       if target_column.cardinality and target_table.row_count else 0)
        
        # High cardinality in target suggests it's a primary key
        if target_ratio > 0.95:
            if source_ratio < 0.5:
                return "many_to_one", 0.8
            elif source_ratio > 0.95:
                return "one_to_one", 0.7
        
        # Similar cardinalities might indicate one-to-one
        if abs(source_ratio - target_ratio) < 0.1 and source_ratio > 0.8:
            return "one_to_one", 0.6
        
        # Default to many-to-one
        return "many_to_one", 0.5
    
    def _has_fk_structure(self, column: Column) -> bool:
        """Check if a column has structural indicators of being a foreign key."""
        # Not a primary key
        if self._is_likely_primary_key(column):
            return False
        
        # Has FK-like naming
        col_name_lower = column.column_name.lower()
        fk_indicators = ['_id', '_key', '_fk', 'ref_', '_ref', '_code']
        
        return any(indicator in col_name_lower for indicator in fk_indicators)
    
    def _find_structural_targets(self, source_column: Column, tables: List[Table]) -> List[Tuple[Table, Column, float]]:
        """Find structural targets for a potential foreign key column."""
        targets = []
        
        # Simple implementation - look for similarly named columns in other tables
        col_name_lower = source_column.column_name.lower()
        
        for table in tables:
            if table.id == source_column.table_id:
                continue
            
            for column in table.columns:
                if self._is_likely_primary_key(column):
                    # Check name similarity
                    similarity = self._calculate_name_similarity(col_name_lower, column.column_name.lower())
                    if similarity > 0.5:
                        targets.append((table, column, similarity * 0.7))
        
        return targets
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two column names."""
        # Simple Jaccard similarity on character n-grams
        def get_ngrams(s: str, n: int = 2) -> Set[str]:
            return set(s[i:i+n] for i in range(len(s) - n + 1))
        
        ngrams1 = get_ngrams(name1)
        ngrams2 = get_ngrams(name2)
        
        if not ngrams1 or not ngrams2:
            return 1.0 if name1 == name2 else 0.0
        
        intersection = ngrams1.intersection(ngrams2)
        union = ngrams1.union(ngrams2)
        
        return len(intersection) / len(union)
    
    def _get_fk_indicators(self, column: Column) -> List[str]:
        """Get list of foreign key indicators for a column."""
        indicators = []
        col_name_lower = column.column_name.lower()
        
        if '_id' in col_name_lower:
            indicators.append('id_suffix')
        if '_key' in col_name_lower:
            indicators.append('key_suffix')
        if '_fk' in col_name_lower:
            indicators.append('fk_suffix')
        if col_name_lower.startswith('ref_'):
            indicators.append('ref_prefix')
        
        return indicators
    
    def _infer_relationship_type(self, source_column: Column, target_column: Column,
                               source_table: Table, target_table: Table) -> str:
        """Infer the type of relationship between two columns."""
        
        # Check cardinalities if available
        if (source_column.cardinality and target_column.cardinality and 
            source_table.row_count and target_table.row_count):
            
            source_uniqueness = source_column.cardinality / source_table.row_count
            target_uniqueness = target_column.cardinality / target_table.row_count
            
            # Both are unique -> one-to-one
            if source_uniqueness > 0.95 and target_uniqueness > 0.95:
                return "one_to_one"
            
            # Target is unique -> many-to-one
            if target_uniqueness > 0.95:
                return "many_to_one"
            
            # Source is unique -> one-to-many
            if source_uniqueness > 0.95:
                return "one_to_many"
        
        # Default assumption
        return "many_to_one"
    
    def _rank_and_deduplicate_candidates(self, candidates: List[RelationshipCandidate]) -> List[RelationshipCandidate]:
        """Rank candidates and remove duplicates."""
        # Group by source-target column pairs
        groups = defaultdict(list)
        
        for candidate in candidates:
            key = (candidate.source_table.id, candidate.source_column.id,
                   candidate.target_table.id, candidate.target_column.id)
            groups[key].append(candidate)
        
        # For each group, take the highest confidence candidate
        final_candidates = []
        for group in groups.values():
            best_candidate = max(group, key=lambda c: c.confidence_score)
            if best_candidate.confidence_score > 0.5:  # Final threshold
                final_candidates.append(best_candidate)
        
        # Sort by confidence
        final_candidates.sort(key=lambda c: c.confidence_score, reverse=True)
        
        return final_candidates
    
    def _save_relationships(self, candidates: List[RelationshipCandidate]) -> List[Relationship]:
        """Save relationship candidates to database."""
        saved_relationships = []
        
        for candidate in candidates:
            # Check if relationship already exists
            existing = self.db.query(Relationship).filter(
                and_(
                    Relationship.source_table_id == candidate.source_table.id,
                    Relationship.source_column_id == candidate.source_column.id,
                    Relationship.target_table_id == candidate.target_table.id,
                    Relationship.target_column_id == candidate.target_column.id
                )
            ).first()
            
            if not existing:
                relationship = Relationship(
                    source_table_id=candidate.source_table.id,
                    source_column_id=candidate.source_column.id,
                    target_table_id=candidate.target_table.id,
                    target_column_id=candidate.target_column.id,
                    relationship_type=candidate.relationship_type,
                    confidence_score=candidate.confidence_score,
                    heuristic_score=candidate.confidence_score,
                    is_validated=False
                )
                
                self.db.add(relationship)
                saved_relationships.append(relationship)
        
        if saved_relationships:
            self.db.commit()
            logger.info(f"Saved {len(saved_relationships)} new relationships")
        
        return saved_relationships