"""Main entry point for Schema Scribe."""

import uvicorn
import logging
import os
from schema_scribe.api.main import app

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Main function to run the application."""
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "127.0.0.1")
    
    print(f"""
    🤖 Schema Scribe - LLM-Powered Data Catalog
    
    Starting server at: http://{host}:{port}
    
    Environment Variables Needed:
    - OPENAI_API_KEY or ANTHROPIC_API_KEY (for AI descriptions)
    - DATABASE_URL (optional, defaults to SQLite)
    
    Ready to catalog your data! 🚀
    """)
    
    uvicorn.run(
        "schema_scribe.api.main:app",
        host=host,
        port=port,
        reload=True
    )

if __name__ == "__main__":
    main()