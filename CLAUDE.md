# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Schema Scribe is an LLM-powered data catalog that automatically generates intelligent schema documentation. It connects to PostgreSQL, SQLite, and Microsoft SQL Server databases, analyzes schemas and data patterns, and uses AI to generate comprehensive, business-friendly documentation with human-in-the-loop validation.

## Development Commands

### Setup and Installation
```bash
# Install in development mode
pip install -e ".[dev]"

# Install dependencies only
pip install -e .
```

### Running the Application
```bash
# Start the web server
python -m schema_scribe.main
# or
schema-scribe

# Default URL: http://localhost:8000
```

### Development Tools
```bash
# Format code
black .
isort .

# Type checking
mypy schema_scribe/

# Linting
flake8 schema_scribe/

# Run tests
pytest
```

## Environment Variables

Required for full functionality:
- `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` - For AI description generation
- `DATABASE_URL` - Optional, defaults to SQLite (format: `postgresql://user:pass@host:port/db`)
- `HOST` - Server host (default: 127.0.0.1)
- `PORT` - Server port (default: 8000)

## Architecture Overview

### Multi-Layer Architecture
1. **Data Layer** (`services/multi_db_connector.py`) - Connects to PostgreSQL, SQLite, and SQL Server
2. **Profiling Layer** (`services/data_profiler.py`) - Analyzes data patterns, cardinality, and quality
3. **AI Layer** (`services/ai_service.py`) - Generates descriptions using OpenAI/Anthropic APIs
4. **Storage Layer** (`models/`) - SQLAlchemy models for metadata and AI content
5. **API Layer** (`api/main.py`) - FastAPI REST endpoints
6. **UI Layer** (`web/templates/`) - Jinja2 templates with Bootstrap

### Key Components

**Models** (`schema_scribe/models/`):
- `catalog.py` - Core entities (DataSource, Table, Column, Relationship)
- `ai_content.py` - AI-generated content and validation workflow
- `base.py` - Database configuration and session management

**Services** (`schema_scribe/services/`):
- `MultiDatabaseConnector` - Multi-database metadata extraction (PostgreSQL, SQLite, SQL Server)
- `DataProfiler` - Column statistics and pattern analysis  
- `AIService` - LLM integration with prompt engineering

**Core Logic** (`schema_scribe/core/`):
- `CatalogManager` - Orchestrates discovery, profiling, and AI generation

### API Endpoints

**Data Sources**:
- `POST /api/data-sources` - Add new data source
- `GET /api/data-sources` - List all data sources
- `POST /api/data-sources/{id}/discover` - Discover schema
- `POST /api/data-sources/{id}/generate-descriptions` - Generate AI descriptions

**Tables & Columns**:
- `GET /api/data-sources/{id}/tables` - List tables
- `GET /api/tables/{id}` - Get table details
- `POST /api/descriptions/{id}/validate` - Validate AI descriptions

**Web Routes**:
- `/` - Data sources overview
- `/catalog/{data_source_id}` - Table catalog
- `/table/{table_id}` - Table details with columns

## Key Workflows

### 1. Schema Discovery
```python
# Connect to database and extract metadata
catalog_manager.discover_schema(data_source_id, schemas=['public'])
```

### 2. AI Description Generation
```python
# Generate descriptions using LLM with profiling context
catalog_manager.generate_descriptions(data_source_id)
```

### 3. Human Validation
```python
# Approve, edit, or reject AI suggestions
catalog_manager.validate_description(description_id, action='approve')
```

## Database Schema

**Key Tables**:
- `data_sources` - Connected databases
- `tables` - Database tables with profiling data
- `columns` - Column metadata and statistics
- `ai_descriptions` - AI-generated content with validation status
- `relationships` - Inferred table relationships (future)

## AI Integration Details

**Prompt Engineering**:
- Few-shot prompting with golden examples
- Chain-of-thought reasoning
- Structured JSON output
- RAG context assembly

**LLM Providers**:
- OpenAI (GPT-4) - Default
- Anthropic (Claude) - Alternative
- Configurable via `LLMProvider` enum

**Context Assembly**:
- Table/column metadata
- Data profiling statistics (cardinality, nulls, patterns)
- Sample values
- Business context (when available)

## Testing Strategy

Run tests with:
```bash
pytest tests/
```

Test categories:
- Unit tests for services and core logic
- Integration tests for database operations
- API endpoint tests
- Mock LLM responses for consistent testing

## Common Development Tasks

### Adding New Data Source Type
1. Extend `DatabaseConnector` with new driver
2. Update `database_type` enum in models
3. Add connection string validation
4. Test metadata extraction

### Extending AI Capabilities
1. Modify prompt templates in `AIService`
2. Update response parsing for new fields
3. Extend `AIDescription` model if needed
4. Add validation logic

### Adding New Relationship Types
1. Extend `Relationship` model
2. Implement detection logic in `DataProfiler`
3. Add UI components for visualization
4. Update API endpoints

## Production Considerations

**Performance**:
- Data profiling runs on samples to avoid large table scans
- Background task queue recommended for large schemas
- Vector database for semantic search (future enhancement)

**Security**:
- Connection strings stored securely
- PII detection and flagging
- Audit trail for all human validations

**Scalability**:
- Horizontal scaling via multiple worker processes
- Database connection pooling
- Caching for frequently accessed metadata