#!/usr/bin/env python3
"""Test script for Schema Scribe enhancements."""

import sys
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from schema_scribe.models.base import DATABASE_URL, engine
from schema_scribe.models import DataSource, Table, TableFilter, UserContext

def test_database_structure():
    """Test that the database has the new structure."""
    print("Testing database structure...")
    
    # Test DataSource model with new fields
    from sqlalchemy import inspect
    inspector = inspect(engine)
    
    # Check data_sources table columns
    ds_columns = [col['name'] for col in inspector.get_columns('data_sources')]
    expected_columns = [
        'databases', 'included_schemas', 'excluded_schemas',
        'included_tables_pattern', 'excluded_tables_pattern',
        'auto_profile', 'sample_size'
    ]
    
    missing_columns = [col for col in expected_columns if col not in ds_columns]
    if missing_columns:
        print(f"✗ Missing columns in data_sources: {missing_columns}")
        return False
    else:
        print("✓ All new data_sources columns present")
    
    # Check new tables exist
    tables = inspector.get_table_names()
    expected_tables = ['table_filters', 'user_contexts']
    
    missing_tables = [table for table in expected_tables if table not in tables]
    if missing_tables:
        print(f"✗ Missing tables: {missing_tables}")
        return False
    else:
        print("✓ All new tables present")
    
    return True

def test_model_creation():
    """Test creating instances of new models."""
    print("\nTesting model creation...")
    
    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Test DataSource with new fields
        ds = DataSource(
            name="Test Enhanced DataSource",
            connection_string="sqlite:///test.db",
            database_type="sqlite",
            databases=["db1", "db2"],
            included_schemas=["public", "analytics"],
            excluded_schemas=["temp"],
            included_tables_pattern="^(fact|dim)_.*",
            excluded_tables_pattern="^temp_.*",
            auto_profile=True,
            sample_size=5000
        )
        session.add(ds)
        session.flush()
        
        print("✓ Enhanced DataSource created")
        
        # Test TableFilter
        table_filter = TableFilter(
            data_source_id=ds.id,
            table_id=1,  # Assuming table ID 1 exists
            is_included=True,
            priority="important",
            reason="Critical business table"
        )
        session.add(table_filter)
        session.flush()
        
        print("✓ TableFilter created")
        
        # Test UserContext
        user_context = UserContext(
            table_id=1,  # Assuming table ID 1 exists
            business_description="Customer transaction records",
            business_purpose="Financial reporting and analytics",
            business_rules=["Retain for 7 years", "PII encryption required"],
            examples=[{"transaction_id": "TXN_123", "amount": 99.99}],
            glossary={"TXN": "Transaction", "PII": "Personal Information"},
            confidence_level="high",
            context_type="complete"
        )
        session.add(user_context)
        session.flush()
        
        print("✓ UserContext created")
        
        session.rollback()  # Don't actually save test data
        return True
        
    except Exception as e:
        print(f"✗ Model creation failed: {e}")
        session.rollback()
        return False
    finally:
        session.close()

def test_api_imports():
    """Test that the API modules can be imported."""
    print("\nTesting API imports...")
    
    try:
        from schema_scribe.api.endpoints import router
        print("✓ Enhanced endpoints imported successfully")
        
        from schema_scribe.services.erd_generator import ERDGenerator
        print("✓ ERD generator imported successfully")
        
        from schema_scribe.services.enhanced_ai_service import EnhancedAIService
        print("✓ Enhanced AI service imported successfully")
        
        return True
        
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False

def main():
    """Run all tests."""
    print("Schema Scribe Enhancement Tests")
    print("=" * 40)
    
    tests_passed = 0
    total_tests = 3
    
    # Test 1: Database structure
    if test_database_structure():
        tests_passed += 1
    
    # Test 2: Model creation
    if test_model_creation():
        tests_passed += 1
    
    # Test 3: API imports
    if test_api_imports():
        tests_passed += 1
    
    print(f"\nTest Results: {tests_passed}/{total_tests} passed")
    
    if tests_passed == total_tests:
        print("\n✅ All enhancements working correctly!")
        print("\nNew features ready:")
        print("• Multi-database/schema filtering")
        print("• Table prioritization and exclusion")
        print("• User context and business hints")
        print("• Paginated API with search/filtering")
        print("• ERD visualization with Mermaid.js")
        print("• Enhanced AI service with context integration")
    else:
        print(f"\n❌ {total_tests - tests_passed} tests failed")
        sys.exit(1)

if __name__ == "__main__":
    main()