#!/usr/bin/env python
"""
Database management utility script.
"""
import sys
import os
import argparse
import sqlite3
from pathlib import Path
import json
import shutil
from datetime import datetime

# Add the parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

from app.models.database import get_db_connection, init_db, apply_migrations
from app.config.settings import DB_PATH, DATA_DIR, JSON_DATA_DIR

def backup_database(output_dir=None):
    """
    Create a backup of the database.
    
    Args:
        output_dir: Directory to store the backup (default: data directory)
    """
    if not os.path.exists(DB_PATH):
        print(f"❌ Database file not found: {DB_PATH}")
        return False
    
    if output_dir is None:
        output_dir = DATA_DIR
    
    # Create backup directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Create timestamp for backup filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(output_dir, f"movies_backup_{timestamp}.db")
    
    # Copy the database file
    try:
        shutil.copy2(DB_PATH, backup_path)
        print(f"✅ Database backup created: {backup_path}")
        return True
    except Exception as e:
        print(f"❌ Error creating backup: {str(e)}")
        return False

def export_data(output_dir=None, tables=None):
    """
    Export database tables to JSON files.
    
    Args:
        output_dir: Directory to store the exported data (default: data/json directory)
        tables: List of tables to export (default: all tables)
    """
    if not os.path.exists(DB_PATH):
        print(f"❌ Database file not found: {DB_PATH}")
        return False
    
    if output_dir is None:
        output_dir = JSON_DATA_DIR
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get list of tables if not specified
    if tables is None:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row['name'] for row in cursor.fetchall() if row['name'] != 'sqlite_sequence']
    
    for table in tables:
        try:
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            
            # Convert rows to list of dictionaries
            data = [dict(row) for row in rows]
            
            # Write to JSON file
            output_file = os.path.join(output_dir, f"{table}.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"✅ Exported {len(data)} rows from '{table}' to {output_file}")
        except Exception as e:
            print(f"❌ Error exporting table '{table}': {str(e)}")
    
    conn.close()
    return True

def main():
    parser = argparse.ArgumentParser(description='Database management utility')
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # init command
    init_parser = subparsers.add_parser('init', help='Initialize the database')
    
    # backup command
    backup_parser = subparsers.add_parser('backup', help='Create a database backup')
    backup_parser.add_argument('--output-dir', type=str, help='Directory to store the backup')
    
    # export command
    export_parser = subparsers.add_parser('export', help='Export database tables to JSON')
    export_parser.add_argument('--output-dir', type=str, help='Directory to store the exported data')
    export_parser.add_argument('--tables', type=str, nargs='+', help='Tables to export')
    
    # migrations command
    migrations_parser = subparsers.add_parser('migrate', help='Apply database migrations')
    
    args = parser.parse_args()
    
    if args.command == 'init':
        init_db()
    elif args.command == 'backup':
        backup_database(args.output_dir)
    elif args.command == 'export':
        export_data(args.output_dir, args.tables)
    elif args.command == 'migrate':
        apply_migrations()
    else:
        parser.print_help()

if __name__ == '__main__':
    main() 