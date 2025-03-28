"""
Database connection and schema management.
"""
import sqlite3
import os
import glob
from pathlib import Path
from ..config.settings import DB_PATH, BASE_DIR

def get_db_connection():
    """
    Create and return a database connection.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Return rows as dictionaries
    return conn

def create_database():
    """
    Initialize the database schema if it doesn't exist.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create migrations table to track applied migrations
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create movies table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title_en TEXT NOT NULL,
            title_cn TEXT,
            show_date TEXT,
            show_time TEXT,
            sold_out BOOLEAN DEFAULT FALSE,
            ticket_url TEXT,
            detail_url TEXT,
            image_url TEXT,
            director TEXT,
            director_cn TEXT,
            year INTEGER,
            cinema TEXT,
            imdb_id TEXT,
            tmdb_id TEXT,
            overview_en TEXT,
            overview_cn TEXT,
            rating REAL,
            douban_url TEXT,
            trailer_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create screenings table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS screenings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title_en TEXT NOT NULL,
            movie_id INTEGER,
            cinema TEXT NOT NULL,
            date TEXT NOT NULL,
            time TEXT NOT NULL,
            sold_out BOOLEAN DEFAULT FALSE,
            ticket_url TEXT,
            FOREIGN KEY (movie_id) REFERENCES movies (id)
        )
    """)

    conn.commit()
    conn.close()
    print("✅ Database 'movies.db' initialized successfully!")

def ensure_data_directory():
    """
    Ensure that data directories exist.
    """
    from ..config.settings import DATA_DIR, JSON_DATA_DIR
    
    DATA_DIR.mkdir(exist_ok=True)
    JSON_DATA_DIR.mkdir(exist_ok=True)
    
    return DATA_DIR, JSON_DATA_DIR

def apply_migrations():
    """
    Apply all pending database migrations.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get list of already applied migrations
    cursor.execute("SELECT name FROM migrations")
    applied_migrations = set(row['name'] for row in cursor.fetchall())
    
    # Get all migration scripts
    migration_dir = BASE_DIR / "migrations"
    migration_files = sorted(glob.glob(str(migration_dir / "*.sql")))
    
    applied_count = 0
    
    for migration_file in migration_files:
        migration_name = os.path.basename(migration_file)
        
        # Skip already applied migrations
        if migration_name in applied_migrations:
            continue
        
        print(f"Applying migration: {migration_name}")
        
        # Read and execute migration script
        with open(migration_file, 'r') as f:
            migration_sql = f.read()
            
            # Split by lines and filter out rollback statements
            migration_lines = []
            for line in migration_sql.split('\n'):
                if line.strip().startswith('-- ROLLBACK'):
                    break
                migration_lines.append(line)
            
            migration_sql = '\n'.join(migration_lines)
            
            try:
                cursor.executescript(migration_sql)
                
                # Record migration as applied
                cursor.execute("INSERT INTO migrations (name) VALUES (?)", (migration_name,))
                conn.commit()
                
                applied_count += 1
                print(f"✅ Migration applied: {migration_name}")
            except Exception as e:
                conn.rollback()
                print(f"❌ Error applying migration {migration_name}: {str(e)}")
                raise
    
    conn.close()
    
    if applied_count > 0:
        print(f"✅ {applied_count} migrations applied successfully!")
    else:
        print("✅ Database is up to date. No migrations needed.")
        
    return applied_count

# Initialize
def init_db():
    """
    Initialize the database and data directories.
    """
    ensure_data_directory()
    create_database()
    apply_migrations() 