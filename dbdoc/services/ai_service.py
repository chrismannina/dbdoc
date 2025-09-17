"""AI service for generating descriptions using LLMs."""

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
class GenerationResult:
    """Result from LLM generation."""
    description: str
    suggested_name: Optional[str]
    confidence_score: float
    reasoning: str
    suggested_is_pii: bool
    suggested_business_domain: Optional[str]
    data_quality_warning: Optional[str]
    model_used: str


class AIService:
    """Service for generating AI descriptions using various LLM providers."""
    
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
    
    def generate_column_description(self, 
                                  table_name: str,
                                  column_name: str,
                                  data_type: str,
                                  is_nullable: bool,
                                  profile_data: Dict[str, Any],
                                  sample_values: List[Any],
                                  table_context: Optional[str] = None) -> GenerationResult:
        """Generate description for a database column."""
        
        context = self._build_column_context(
            table_name, column_name, data_type, is_nullable,
            profile_data, sample_values, table_context
        )
        
        prompt = self._build_column_prompt(context)
        
        if self.provider == LLMProvider.OPENAI:
            response = self._call_openai(prompt)
        else:
            response = self._call_anthropic(prompt)
        
        return self._parse_response(response, self.model)
    
    def generate_table_description(self,
                                 schema_name: str,
                                 table_name: str,
                                 columns: List[Dict[str, Any]],
                                 row_count: Optional[int] = None) -> GenerationResult:
        """Generate description for a database table."""
        
        context = self._build_table_context(schema_name, table_name, columns, row_count)
        prompt = self._build_table_prompt(context)
        
        if self.provider == LLMProvider.OPENAI:
            response = self._call_openai(prompt)
        else:
            response = self._call_anthropic(prompt)
        
        return self._parse_response(response, self.model)
    
    def _build_column_context(self, table_name: str, column_name: str, 
                            data_type: str, is_nullable: bool,
                            profile_data: Dict[str, Any], 
                            sample_values: List[Any],
                            table_context: Optional[str]) -> str:
        """Build context for column description generation."""
        
        context_parts = [
            f"Table: {table_name}",
            f"Column: {column_name}",
            f"Data Type: {data_type}",
            f"Nullable: {'Yes' if is_nullable else 'No'}",
        ]
        
        # Add profiling data
        if profile_data:
            if 'cardinality' in profile_data and profile_data['cardinality'] is not None:
                context_parts.append(f"Distinct Values: {profile_data['cardinality']:,}")
            if 'null_percentage' in profile_data and profile_data['null_percentage'] is not None:
                context_parts.append(f"Null Percentage: {profile_data['null_percentage']:.1f}%")
            if 'top_values' in profile_data and profile_data['top_values']:
                top_vals = ", ".join([f"'{v[0]}' ({v[1]})" for v in profile_data['top_values'][:5]])
                context_parts.append(f"Top Values: {top_vals}")
            if 'min_value' in profile_data and profile_data['min_value']:
                context_parts.append(f"Min Value: {profile_data['min_value']}")
            if 'max_value' in profile_data and profile_data['max_value']:
                context_parts.append(f"Max Value: {profile_data['max_value']}")
        
        # Add sample values
        if sample_values:
            sample_str = ", ".join([f"'{v}'" for v in sample_values[:10]])
            context_parts.append(f"Sample Values: {sample_str}")
        
        # Add table context if available
        if table_context:
            context_parts.append(f"Table Context: {table_context}")
        
        return "\\n".join(context_parts)
    
    def _build_table_context(self, schema_name: str, table_name: str,
                           columns: List[Dict[str, Any]], row_count: Optional[int]) -> str:
        """Build context for table description generation."""
        
        context_parts = [
            f"Schema: {schema_name}",
            f"Table: {table_name}",
        ]
        
        if row_count is not None:
            context_parts.append(f"Row Count: {row_count:,}")
        
        context_parts.append(f"Number of Columns: {len(columns)}")
        
        # Add column summary
        if columns:
            context_parts.append("\\nColumns:")
            for col in columns[:20]:  # Limit to first 20 columns
                col_line = f"  - {col['column_name']} ({col['data_type']})"
                if col.get('description'):
                    col_line += f": {col['description'][:100]}"
                context_parts.append(col_line)
        
        return "\\n".join(context_parts)
    
    def _build_column_prompt(self, context: str) -> str:
        """Build prompt for column description generation."""
        
        return f"""You are an expert data analyst creating a world-class data dictionary. 
Your task is to analyze the provided context for a database column and generate a comprehensive, structured description in JSON format.

### INSTRUCTIONS ###
Based on the provided context, analyze the target column and generate a JSON output with your reasoning and the final description.

### EXAMPLES ###

Example 1:
Context:
- Table: users, Column: user_id
- Data Type: integer
- Nullable: No
- Distinct Values: 1,500,000
- Null Percentage: 0.0%
- Sample Values: 101, 102, 103, 104, 105

Output:
```json
{{
  "reasoning": "The column is a non-nullable integer with 100% unique values, and its name is 'user_id'. This strongly indicates it is the primary key for the 'users' table, uniquely identifying each user record.",
  "suggested_name": "User ID",
  "description": "A unique system-generated identifier for each user in the platform.",
  "is_pii": false,
  "business_domain": null,
  "data_quality_warning": null,
  "confidence_score": 0.95
}}
```

Example 2:
Context:
- Table: orders, Column: customer_email
- Data Type: varchar
- Nullable: Yes
- Distinct Values: 45,230
- Null Percentage: 2.3%
- Sample Values: 'john@example.com', 'sarah.smith@company.org', 'mike.jones@email.net'

Output:
```json
{{
  "reasoning": "The column contains email addresses based on the sample values and column name. The 2.3% null rate suggests most orders have associated customer emails, but some may be from guest checkouts.",
  "suggested_name": "Customer Email Address", 
  "description": "The email address of the customer who placed the order, used for communication and account identification.",
  "is_pii": true,
  "business_domain": "customer",
  "data_quality_warning": "2.3% of records have missing email addresses",
  "confidence_score": 0.92
}}
```

### TARGET COLUMN CONTEXT ###
{context}

### TASK ###
Generate the JSON output for the target column. Ensure your response is valid JSON and includes all required fields.
"""

    def _build_table_prompt(self, context: str) -> str:
        """Build prompt for table description generation."""
        
        return f"""You are an expert data analyst creating documentation for a database table.
Analyze the provided context and generate a comprehensive description in JSON format.

### CONTEXT ###
{context}

### TASK ###
Generate a JSON response with the following structure:
```json
{{
  "reasoning": "Your step-by-step analysis of what this table represents",
  "suggested_name": "Business-friendly name for the table",
  "description": "Clear, concise description of what this table stores and its business purpose",
  "business_domain": "Primary business domain (e.g., finance, customer, operations)",
  "data_quality_warning": "Any data quality concerns or null if none",
  "confidence_score": "Confidence in the description from 0.0 to 1.0"
}}
```

Focus on the business purpose and what real-world entities or events this table represents.
"""

    def _call_openai(self, prompt: str) -> str:
        """Call OpenAI API."""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert data analyst. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=1000
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"OpenAI API call failed: {e}")
            raise
    
    def _call_anthropic(self, prompt: str) -> str:
        """Call Anthropic API."""
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1000,
                temperature=0.1,
                system="You are an expert data analyst. Always respond with valid JSON.",
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        except Exception as e:
            logger.error(f"Anthropic API call failed: {e}")
            raise
    
    def _parse_response(self, response: str, model_used: str) -> GenerationResult:
        """Parse LLM response into GenerationResult."""
        try:
            # Extract JSON from response if it's wrapped in markdown
            if "```json" in response:
                start = response.find("```json") + 7
                end = response.find("```", start)
                response = response[start:end].strip()
            elif "```" in response:
                start = response.find("```") + 3
                end = response.find("```", start)
                response = response[start:end].strip()
            
            data = json.loads(response)
            
            return GenerationResult(
                description=data.get("description", ""),
                suggested_name=data.get("suggested_name"),
                confidence_score=float(data.get("confidence_score", 0.5)),
                reasoning=data.get("reasoning", ""),
                suggested_is_pii=data.get("is_pii", False),
                suggested_business_domain=data.get("business_domain"),
                data_quality_warning=data.get("data_quality_warning"),
                model_used=model_used
            )
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Failed to parse LLM response: {e}")
            logger.error(f"Response was: {response}")
            
            # Fallback result
            return GenerationResult(
                description="Failed to generate description",
                suggested_name=None,
                confidence_score=0.0,
                reasoning=f"Error parsing response: {e}",
                suggested_is_pii=False,
                suggested_business_domain=None,
                data_quality_warning="AI generation failed",
                model_used=model_used
            )