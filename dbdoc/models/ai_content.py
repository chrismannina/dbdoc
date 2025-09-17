"""Models for AI-generated content and validation workflow."""

from sqlalchemy import Column as SQLColumn, Integer, String, DateTime, Float, Boolean, Text, ForeignKey, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from enum import Enum as PyEnum
from .base import Base


class ValidationStatus(PyEnum):
    """Status of AI-generated content validation."""
    PENDING = "pending"
    APPROVED = "approved"
    EDITED = "edited"
    REJECTED = "rejected"


class AIDescription(Base):
    """AI-generated descriptions for tables and columns."""
    
    __tablename__ = "ai_descriptions"
    
    id = SQLColumn(Integer, primary_key=True, index=True)
    
    # Link to either table or column
    table_id = SQLColumn(Integer, ForeignKey("tables.id"), nullable=True)
    column_id = SQLColumn(Integer, ForeignKey("columns.id"), nullable=True)
    
    # AI-generated content
    suggested_name = SQLColumn(String(255))  # Business-friendly name
    description = SQLColumn(Text, nullable=False)
    confidence_score = SQLColumn(Float)  # AI confidence 0-1
    
    # RAG context used
    context_used = SQLColumn(Text)  # What context was provided to LLM
    reasoning = SQLColumn(Text)     # LLM's chain of thought
    
    # Validation workflow
    status = SQLColumn(Enum(ValidationStatus), default=ValidationStatus.PENDING)
    human_feedback = SQLColumn(Text)  # If edited or rejected, why?
    final_description = SQLColumn(Text)  # Human-approved final description
    
    # Classification suggestions
    suggested_business_domain = SQLColumn(String(100))
    suggested_is_pii = SQLColumn(Boolean)
    suggested_data_quality_warning = SQLColumn(Text)
    
    # Metadata
    created_at = SQLColumn(DateTime(timezone=True), server_default=func.now())
    validated_at = SQLColumn(DateTime(timezone=True))
    validated_by = SQLColumn(String(255))  # User who validated
    
    # Model info
    model_used = SQLColumn(String(100))  # Which LLM model generated this
    prompt_version = SQLColumn(String(50))  # Track prompt iterations
    
    # Relationships
    table = relationship("Table", back_populates="ai_descriptions")
    column = relationship("Column", back_populates="ai_descriptions")