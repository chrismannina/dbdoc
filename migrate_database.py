#!/usr/bin/env python3
"""Database migration script for Schema Scribe enhancements."""

import os
import sys
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from schema_scribe.models.base import DATABASE_URL, engine
from schema_scribe.models import Base

def check_table_exists(engine, table_name):
    """Check if a table exists in the database."""
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()

def check_column_exists(engine, table_name, column_name):
    """Check if a column exists in a table."""
    inspector = inspect(engine)
    if not check_table_exists(engine, table_name):
        return False
    
    columns = inspector.get_columns(table_name)
    return any(col['name'] == column_name for col in columns)

def add_column_if_not_exists(engine, table_name, column_definition):
    """Add a column to a table if it doesn't exist."""
    column_name = column_definition.split()[0]
    
    if not check_column_exists(engine, table_name, column_name):
        try:
            with engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_definition}"))
                conn.commit()
                print(f"✓ Added column {column_name} to {table_name}")
        except Exception as e:
            print(f"✗ Failed to add column {column_name} to {table_name}: {e}")
    else:
        print(f"→ Column {column_name} already exists in {table_name}")

def migrate_data_sources():
    """Migrate data_sources table to add new columns."""
    print("Migrating data_sources table...")
    
    # Add new columns to data_sources table
    new_columns = [
        "databases JSON",
        "included_schemas JSON", 
        "excluded_schemas JSON",
        "included_tables_pattern VARCHAR(500)",
        "excluded_tables_pattern VARCHAR(500)",
        "auto_profile BOOLEAN DEFAULT 1",
        "sample_size INTEGER DEFAULT 10000"
    ]
    
    for column_def in new_columns:
        add_column_if_not_exists(engine, 'data_sources', column_def)

def create_new_tables():
    """Create new tables if they don't exist."""
    print("Creating new tables...")
    
    new_tables = ['table_filters', 'user_contexts']
    
    for table_name in new_tables:
        if not check_table_exists(engine, table_name):
            print(f"Creating table {table_name}...")
        else:
            print(f"→ Table {table_name} already exists")
    
    # Create all tables (will only create missing ones)
    Base.metadata.create_all(bind=engine)
    print("✓ All tables created/verified")

def main():
    """Run database migration."""
    print("Schema Scribe Database Migration")
    print("=" * 40)
    print(f"Database URL: {DATABASE_URL}")
    print()
    
    try:
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✓ Database connection successful")
        print()
        
        # Run migrations
        migrate_data_sources()
        print()
        
        create_new_tables() 
        print()
        
        print("✓ Migration completed successfully!")
        print()
        print("New features available:")
        print("- Multi-database/schema filtering")
        print("- Table inclusion/exclusion with priorities")
        print("- User context and business hints")
        print("- Enhanced pagination and search")
        print("- ERD visualization")
        
    except Exception as e:
        print(f"✗ Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()