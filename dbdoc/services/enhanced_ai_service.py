"""Enhanced AI service with user context support."""

import logging
import os
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import openai
import anthropic
from enum import Enum

logger = logging.getLogger(__name__)


class LLMProvider(Enum):
    """Supported LLM providers."""
    OPENAI = "openai"
    ANTHROPIC = "anthropic"


@dataclass
class EnhancedGenerationResult:
    """Enhanced result from LLM generation with user context."""
    description: str
    suggested_name: Optional[str]
    confidence_score: float
    reasoning: str
    suggested_is_pii: bool
    suggested_business_domain: Optional[str]
    data_quality_warning: Optional[str]
    model_used: str
    used_user_context: bool
    context_influence_score: float  # How much user context influenced the result


class EnhancedAIService:
    """Enhanced AI service with user context integration."""
    
    def __init__(self, provider: LLMProvider = LLMProvider.OPENAI):
        """Initialize AI service with specified provider."""
        self.provider = provider
        
        if provider == LLMProvider.OPENAI:
            self.client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            self.model = "gpt-4"
        elif provider == LLMProvider.ANTHROPIC:
            self.client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
            self.model = "claude-3-sonnet-20240229"
        else:
            raise ValueError(f"Unsupported provider: {provider}")
    
    def generate_with_context(self,
                             entity_type: str,  # "table" or "column"
                             entity_metadata: Dict[str, Any],
                             profile_data: Dict[str, Any],
                             user_context: Optional[Dict[str, Any]] = None,
                             relationships: Optional[List[Dict[str, Any]]] = None) -> EnhancedGenerationResult:
        """Generate description with user context integration."""
        
        # Build enhanced context
        context = self._build_enhanced_context(
            entity_type, entity_metadata, profile_data, user_context, relationships
        )
        
        # Generate prompt with user hints
        prompt = self._build_contextual_prompt(entity_type, context, user_context)
        
        # Call LLM
        if self.provider == LLMProvider.OPENAI:
            response = self._call_openai(prompt)
        else:
            response = self._call_anthropic(prompt)
        
        # Parse and enhance response
        result = self._parse_enhanced_response(response, self.model, bool(user_context))
        
        # Calculate context influence
        if user_context:
            result.context_influence_score = self._calculate_context_influence(
                user_context, result.description
            )
        
        return result
    
    def iterate_with_feedback(self,
                             existing_description: str,
                             feedback: str,
                             additional_context: Dict[str, Any]) -> EnhancedGenerationResult:
        """Iterate on existing description with user feedback."""
        
        prompt = f"""
        You previously generated this description:
        {existing_description}
        
        The user provided this feedback:
        {feedback}
        
        Additional context provided:
        {json.dumps(additional_context, indent=2)}
        
        Please generate an improved description that addresses the feedback and incorporates the new context.
        
        Return as JSON:
        {{
            "description": "improved description",
            "changes_made": "explanation of what was changed and why",
            "confidence_score": 0.0-1.0
        }}
        """
        
        if self.provider == LLMProvider.OPENAI:
            response = self._call_openai(prompt)
        else:
            response = self._call_anthropic(prompt)
        
        parsed = json.loads(response)
        
        return EnhancedGenerationResult(
            description=parsed['description'],
            suggested_name=None,
            confidence_score=parsed['confidence_score'],
            reasoning=parsed['changes_made'],
            suggested_is_pii=False,
            suggested_business_domain=None,
            data_quality_warning=None,
            model_used=self.model,
            used_user_context=True,
            context_influence_score=0.8  # High influence from direct feedback
        )
    
    def _build_enhanced_context(self,
                               entity_type: str,
                               entity_metadata: Dict[str, Any],
                               profile_data: Dict[str, Any],
                               user_context: Optional[Dict[str, Any]],
                               relationships: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Build enhanced context combining all information sources."""
        
        context = {
            "entity_type": entity_type,
            "metadata": entity_metadata,
            "profile": profile_data,
            "relationships": relationships or []
        }
        
        # Merge user context if provided
        if user_context:
            context["user_provided"] = {
                "business_description": user_context.get("business_description"),
                "business_purpose": user_context.get("business_purpose"),
                "data_sources": user_context.get("data_sources"),
                "data_consumers": user_context.get("data_consumers"),
                "business_rules": user_context.get("business_rules", []),
                "examples": user_context.get("examples", []),
                "glossary": user_context.get("glossary", {}),
                "notes": user_context.get("notes"),
                "confidence_level": user_context.get("confidence_level", "medium")
            }
        
        return context
    
    def _build_contextual_prompt(self,
                                entity_type: str,
                                context: Dict[str, Any],
                                user_context: Optional[Dict[str, Any]]) -> str:
        """Build prompt that emphasizes user context when available."""
        
        if entity_type == "table":
            base_prompt = self._build_table_prompt_template()
        else:
            base_prompt = self._build_column_prompt_template()
        
        # Add user context emphasis if provided
        if user_context:
            context_emphasis = """
            IMPORTANT: The user has provided business context for this entity.
            Give HIGH WEIGHT to the user-provided information when generating the description.
            The user context should be the PRIMARY source for understanding the business meaning.
            
            User Context Provided:
            """
            
            if user_context.get("business_description"):
                context_emphasis += f"\nBusiness Description: {user_context['business_description']}"
            if user_context.get("business_purpose"):
                context_emphasis += f"\nBusiness Purpose: {user_context['business_purpose']}"
            if user_context.get("glossary"):
                context_emphasis += f"\nBusiness Terms: {json.dumps(user_context['glossary'])}"
            if user_context.get("examples"):
                context_emphasis += f"\nExamples: {json.dumps(user_context['examples'][:3])}"
            
            base_prompt = context_emphasis + "\n\n" + base_prompt
        
        # Format with context
        return base_prompt.format(context=json.dumps(context, indent=2))
    
    def _build_table_prompt_template(self) -> str:
        """Template for table description generation."""
        return """
        Generate a comprehensive business-friendly description for this database table.
        
        Context:
        {context}
        
        Requirements:
        1. Focus on business meaning and purpose
        2. Explain what data this table stores and why
        3. Describe key relationships to other tables
        4. Mention data quality considerations if relevant
        5. Use terminology from the glossary if provided
        
        Return as JSON:
        {{
            "description": "2-3 sentence business description",
            "suggested_name": "optional better name if current is unclear",
            "confidence_score": 0.0-1.0,
            "reasoning": "explanation of how you arrived at this description",
            "suggested_business_domain": "e.g., sales, inventory, customer",
            "data_quality_warning": "any concerns about the data"
        }}
        """
    
    def _build_column_prompt_template(self) -> str:
        """Template for column description generation."""
        return """
        Generate a comprehensive business-friendly description for this database column.
        
        Context:
        {context}
        
        Requirements:
        1. Explain what this column represents in business terms
        2. Describe valid values and their meanings
        3. Note any business rules or constraints
        4. Flag if this appears to be PII
        5. Use terminology from the glossary if provided
        
        Return as JSON:
        {{
            "description": "1-2 sentence business description",
            "suggested_name": "optional better name if current is unclear",
            "confidence_score": 0.0-1.0,
            "reasoning": "explanation of how you arrived at this description",
            "suggested_is_pii": true/false,
            "suggested_business_domain": "e.g., sales, inventory, customer",
            "data_quality_warning": "any concerns about the data"
        }}
        """
    
    def _call_openai(self, prompt: str) -> str:
        """Call OpenAI API."""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a data catalog expert helping document database schemas."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1000
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            raise
    
    def _call_anthropic(self, prompt: str) -> str:
        """Call Anthropic API."""
        try:
            response = self.client.messages.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                max_tokens=1000,
                temperature=0.3
            )
            return response.content[0].text
        except Exception as e:
            logger.error(f"Anthropic API error: {e}")
            raise
    
    def _parse_enhanced_response(self, response: str, model: str, used_context: bool) -> EnhancedGenerationResult:
        """Parse LLM response into enhanced result."""
        try:
            parsed = json.loads(response)
            
            return EnhancedGenerationResult(
                description=parsed.get("description", ""),
                suggested_name=parsed.get("suggested_name"),
                confidence_score=float(parsed.get("confidence_score", 0.5)),
                reasoning=parsed.get("reasoning", ""),
                suggested_is_pii=parsed.get("suggested_is_pii", False),
                suggested_business_domain=parsed.get("suggested_business_domain"),
                data_quality_warning=parsed.get("data_quality_warning"),
                model_used=model,
                used_user_context=used_context,
                context_influence_score=0.0  # Will be calculated separately
            )
        except json.JSONDecodeError:
            # Fallback for non-JSON response
            return EnhancedGenerationResult(
                description=response,
                suggested_name=None,
                confidence_score=0.3,
                reasoning="Could not parse structured response",
                suggested_is_pii=False,
                suggested_business_domain=None,
                data_quality_warning=None,
                model_used=model,
                used_user_context=used_context,
                context_influence_score=0.0
            )
    
    def _calculate_context_influence(self, user_context: Dict[str, Any], description: str) -> float:
        """Calculate how much user context influenced the description."""
        influence_score = 0.0
        influence_factors = 0
        
        # Check if key terms from user context appear in description
        if user_context.get("business_description"):
            key_terms = user_context["business_description"].lower().split()
            matches = sum(1 for term in key_terms if term in description.lower())
            influence_score += min(matches / len(key_terms), 1.0)
            influence_factors += 1
        
        if user_context.get("glossary"):
            glossary_terms = list(user_context["glossary"].keys())
            matches = sum(1 for term in glossary_terms if term.lower() in description.lower())
            if glossary_terms:
                influence_score += min(matches / len(glossary_terms), 1.0)
                influence_factors += 1
        
        # Average the influence scores
        if influence_factors > 0:
            return influence_score / influence_factors
        
        return 0.0