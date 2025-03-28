#!/usr/bin/env python
"""
Application entry point.
"""
import sys
import os
from pathlib import Path
import argparse

# Add the parent directory to sys.path
parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))

from app.models.database import init_db
from app.api.server import create_app, run_server

def main():
    """
    Main entry point for the application.
    """
    parser = argparse.ArgumentParser(description='WhatsgoodinNYCcinema Backend Server')
    parser.add_argument('--init-db', action='store_true', help='Initialize the database')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    
    args = parser.parse_args()
    
    if args.init_db:
        init_db()
        print("Database initialized successfully!")
        return
        
    app = create_app()
    run_server(app, host=args.host, port=args.port, debug=args.debug)

if __name__ == "__main__":
    main() 