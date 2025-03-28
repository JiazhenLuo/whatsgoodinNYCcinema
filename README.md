# What's Good in NYC Cinema

A web application that helps users discover and track movies playing in NYC theaters. The application provides information about current and upcoming movies, including showtimes, ratings, and reviews.

## Features

- Movie information from multiple sources (TMDB, OMDb)
- Showtimes for NYC theaters
- Movie ratings and reviews
- Chinese and English title support
- Automatic title updates and corrections

## Project Structure

```
.
├── backend/
│   ├── app/
│   │   ├── api/         # API endpoints
│   │   ├── models/      # Database models
│   │   └── services/    # Business logic
│   ├── config/          # Configuration files
│   └── scripts/         # Utility scripts
├── frontend/            # Frontend application
└── tests/              # Test files
```

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your API keys and configuration
```

4. Initialize the database:
```bash
python backend/scripts/init_db.py
```

5. Run the Flask server:
```bash
python backend/run_flask.py
```

## API Documentation

The API is available at `http://localhost:5556/api/v1/`

### Endpoints

- `GET /api/v1/movies` - List all movies
- `GET /api/v1/movies/{id}` - Get movie details
- `POST /api/v1/movies/{id}/refresh` - Refresh movie information
- `GET /api/v1/health` - Health check endpoint

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 