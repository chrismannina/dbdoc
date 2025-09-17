#!/usr/bin/env python3
"""Clean up test databases and Schema Scribe's internal database."""

import os
import glob

def cleanup_test_databases():
    """Remove test databases and Schema Scribe's internal database."""
    
    files_to_remove = [
        "test_ecommerce.db",
        "dbdoc.db",  # Schema Scribe's internal database
        "*.db-journal",      # SQLite journal files
        "*.db-wal",         # SQLite WAL files
        "*.db-shm"          # SQLite shared memory files
    ]
    
    removed_count = 0
    
    print("🧹 Cleaning up test databases...")
    
    for pattern in files_to_remove:
        for file_path in glob.glob(pattern):
            try:
                os.remove(file_path)
                print(f"  ✅ Removed: {file_path}")
                removed_count += 1
            except OSError as e:
                print(f"  ❌ Failed to remove {file_path}: {e}")
    
    if removed_count == 0:
        print("  ℹ️  No database files found to clean up")
    else:
        print(f"  🎉 Cleaned up {removed_count} files")

def main():
    """Main cleanup function."""
    print("🚀 Schema Scribe Database Cleanup")
    print("=" * 35)
    
    cleanup_test_databases()
    
    print("\n✨ Cleanup complete!")
    print("You can now run create_test_db.py to create a fresh test database.")

if __name__ == "__main__":
    main()