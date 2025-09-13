"""Data profiler for analyzing column statistics and patterns."""

import logging
import re
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy import text
from sqlalchemy.engine import Engine
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)


@dataclass
class ColumnProfile:
    """Profiling results for a column."""
    cardinality: int
    null_percentage: float
    top_values: List[Tuple[Any, int]]  # (value, count) pairs
    min_value: Optional[str]
    max_value: Optional[str]
    avg_value: Optional[float]
    std_dev: Optional[float]
    sample_values: List[Any]
    pattern_analysis: Dict[str, Any]


class DataProfiler:
    """Profiles database columns to understand data patterns and quality."""
    
    def __init__(self, engine: Engine, database_type: str = "postgresql"):
        """Initialize with database engine."""
        self.engine = engine
        self.database_type = database_type
        
        # Common patterns for classification
        self.patterns = {
            'email': re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            'phone': re.compile(r'^\+?[\d\s\-\(\)]{10,}$'),
            'uuid': re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE),
            'url': re.compile(r'^https?://[^\s]+$'),
            'ip_address': re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'),
            'credit_card': re.compile(r'^\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}$'),
            'ssn': re.compile(r'^\d{3}-?\d{2}-?\d{4}$'),
            'date_string': re.compile(r'^\d{4}-\d{2}-\d{2}'),
        }
    
    def profile_column(self, schema_name: str, table_name: str, 
                      column_name: str, sample_size: int = 1000) -> ColumnProfile:
        """Profile a single column."""
        logger.info(f"Profiling column {schema_name}.{table_name}.{column_name}")
        
        # Get basic statistics
        stats_query = text(f'''
            WITH column_stats AS (
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT("{column_name}") as non_null_rows,
                    COUNT(DISTINCT "{column_name}") as distinct_values,
                    MIN("{column_name}"::text) as min_val,
                    MAX("{column_name}"::text) as max_val
                FROM "{schema_name}"."{table_name}"
            ),
            numeric_stats AS (
                SELECT 
                    AVG(CASE WHEN "{column_name}"::text ~ '^-?\\d+(\\.\\d+)?$' 
                        THEN "{column_name}"::text::numeric ELSE NULL END) as avg_val,
                    STDDEV(CASE WHEN "{column_name}"::text ~ '^-?\\d+(\\.\\d+)?$' 
                        THEN "{column_name}"::text::numeric ELSE NULL END) as std_val
                FROM "{schema_name}"."{table_name}"
                WHERE "{column_name}" IS NOT NULL
            )
            SELECT 
                total_rows,
                non_null_rows,
                distinct_values,
                min_val,
                max_val,
                avg_val,
                std_val,
                CASE WHEN total_rows = 0 THEN 0 
                     ELSE ROUND((total_rows - non_null_rows)::numeric / total_rows * 100, 2) 
                END as null_percentage
            FROM column_stats, numeric_stats
        ''')
        
        with self.engine.connect() as conn:
            result = conn.execute(stats_query)
            stats = result.fetchone()
            
            if not stats or stats.total_rows == 0:
                return ColumnProfile(
                    cardinality=0,
                    null_percentage=100.0,
                    top_values=[],
                    min_value=None,
                    max_value=None,
                    avg_value=None,
                    std_dev=None,
                    sample_values=[],
                    pattern_analysis={}
                )
            
            # Get top values
            top_values_query = text(f'''
                SELECT "{column_name}", COUNT(*) as freq
                FROM "{schema_name}"."{table_name}"
                WHERE "{column_name}" IS NOT NULL
                GROUP BY "{column_name}"
                ORDER BY freq DESC
                LIMIT 10
            ''')
            
            top_values_result = conn.execute(top_values_query)
            top_values = [(row[0], row[1]) for row in top_values_result]
            
            # Get sample values for pattern analysis
            sample_query = text(f'''
                SELECT "{column_name}"
                FROM "{schema_name}"."{table_name}"
                WHERE "{column_name}" IS NOT NULL
                ORDER BY RANDOM()
                LIMIT :sample_size
            ''')
            
            sample_result = conn.execute(sample_query, {"sample_size": sample_size})
            sample_values = [row[0] for row in sample_result]
            
            # Analyze patterns
            pattern_analysis = self._analyze_patterns(sample_values)
            
            return ColumnProfile(
                cardinality=stats.distinct_values,
                null_percentage=float(stats.null_percentage),
                top_values=top_values,
                min_value=stats.min_val,
                max_value=stats.max_val,
                avg_value=float(stats.avg_val) if stats.avg_val else None,
                std_dev=float(stats.std_val) if stats.std_val else None,
                sample_values=sample_values[:20],  # Keep first 20 for context
                pattern_analysis=pattern_analysis
            )
    
    def _analyze_patterns(self, values: List[Any]) -> Dict[str, Any]:
        """Analyze data patterns in sample values."""
        if not values:
            return {}
            
        pattern_matches = {}
        string_values = [str(v) for v in values if v is not None]
        
        for pattern_name, pattern in self.patterns.items():
            matches = sum(1 for v in string_values if pattern.match(v))
            if matches > 0:
                pattern_matches[pattern_name] = {
                    'matches': matches,
                    'percentage': round(matches / len(string_values) * 100, 2)
                }
        
        # Additional analysis
        analysis = {
            'pattern_matches': pattern_matches,
            'avg_length': round(sum(len(str(v)) for v in string_values) / len(string_values), 2) if string_values else 0,
            'all_numeric': all(str(v).replace('.', '').replace('-', '').isdigit() for v in string_values),
            'all_uppercase': all(str(v).isupper() for v in string_values if str(v).isalpha()),
            'all_lowercase': all(str(v).islower() for v in string_values if str(v).isalpha()),
        }
        
        return analysis
    
    def classify_column(self, column_name: str, data_type: str, 
                       profile: ColumnProfile) -> Dict[str, Any]:
        """Classify column based on name, type, and data patterns."""
        classification = {
            'is_pii': False,
            'is_key': False,
            'business_domain': None,
            'confidence': 0.0
        }
        
        column_lower = column_name.lower()
        
        # Check for PII indicators
        pii_indicators = [
            'email', 'phone', 'ssn', 'social_security', 'passport',
            'credit_card', 'license', 'address', 'name', 'first_name',
            'last_name', 'full_name', 'dob', 'birth_date'
        ]
        
        if any(indicator in column_lower for indicator in pii_indicators):
            classification['is_pii'] = True
            classification['confidence'] += 0.3
        
        # Check pattern matches for PII
        if profile.pattern_analysis.get('pattern_matches'):
            pii_patterns = ['email', 'phone', 'ssn', 'credit_card']
            for pattern in pii_patterns:
                if pattern in profile.pattern_analysis['pattern_matches']:
                    classification['is_pii'] = True
                    classification['confidence'] += 0.4
        
        # Check for key indicators
        key_indicators = ['id', 'key', 'pk', 'uuid']
        if (any(indicator in column_lower for indicator in key_indicators) or
            profile.cardinality == profile.cardinality):  # Assuming we have row count
            classification['is_key'] = True
            classification['confidence'] += 0.2
        
        # Business domain classification
        domain_keywords = {
            'finance': ['revenue', 'cost', 'price', 'amount', 'payment', 'invoice'],
            'marketing': ['campaign', 'lead', 'conversion', 'click', 'impression'],
            'operations': ['order', 'shipment', 'inventory', 'product', 'sku'],
            'hr': ['employee', 'salary', 'department', 'manager', 'hire_date'],
            'customer': ['customer', 'user', 'client', 'account', 'contact']
        }
        
        for domain, keywords in domain_keywords.items():
            if any(keyword in column_lower for keyword in keywords):
                classification['business_domain'] = domain
                classification['confidence'] += 0.1
                break
        
        return classification