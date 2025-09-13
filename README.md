# Schema Scribe

An LLM-powered data catalog that automatically generates intelligent schema documentation.

## Overview

Schema Scribe transforms database documentation from a manual chore into an intelligent, automated process. It connects to your databases, analyzes schemas and data patterns, and uses AI to generate comprehensive, business-friendly documentation.

## Features

- **Multi-Database Support**: Connects to PostgreSQL, SQLite, and Microsoft SQL Server
- **Automatic Schema Discovery**: Extracts metadata and analyzes database structure
- **Data Profiling**: Analyzes column statistics, patterns, and data quality
- **AI-Generated Descriptions**: Uses LLMs to create human-readable table and column descriptions
- **Human-in-the-Loop**: Review and approve AI suggestions for accuracy
- **Web Interface**: Browse and search your data catalog

## Quick Start

1. Install dependencies:
   ```bash
   pip install -e .
   ```

2. Set up environment variables:
   ```bash
   export OPENAI_API_KEY="your-api-key"
   # Optional: Set default database URL
   export DATABASE_URL="postgresql://user:pass@host:port/db"
   ```

3. Run the application:
   ```bash
   schema-scribe
   ```

4. Open http://localhost:8000 in your browser

## Supported Databases

### PostgreSQL
```
postgresql://username:password@host:port/database
```

### SQLite
```
sqlite:///path/to/database.db
```

### Microsoft SQL Server
```
mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server
```

**Note**: For SQL Server, you'll need to install the ODBC driver separately.

## Development

Install development dependencies:
```bash
pip install -e ".[dev]"
```

Run tests:
```bash
pytest
```

Format code:
```bash
black .
isort .
```

## Architecture

Schema Scribe follows a multi-layer architecture:

1. **Data Layer**: Connects to source databases and extracts metadata
2. **Profiling Layer**: Analyzes data patterns and quality
3. **AI Layer**: Generates descriptions using LLMs with contextual prompts
4. **Storage Layer**: Stores metadata and AI-generated content
5. **API Layer**: REST endpoints for frontend interaction
6. **UI Layer**: Web interface for browsing and curation