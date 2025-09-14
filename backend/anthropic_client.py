"""
Anthropic Claude API Client - LogInsight Agent
Handles communication with Claude API for log cleaning and normalization
"""
import os
import json
import time
from typing import Dict, List, Any, Optional
from anthropic import Anthropic
import asyncio

class ClaudeLogProcessor:
    """Handles Claude API interactions for log processing"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable is required")
        
        try:
            self.client = Anthropic(api_key=self.api_key)
        except Exception as e:
            print(f"Warning: Failed to initialize Anthropic client: {e}")
            self.client = None
        
        self.model = "claude-3-haiku-20240307"  # Fast and efficient for log processing
        
        # Processing statistics
        self.stats = {
            'total_api_calls': 0,
            'successful_calls': 0,
            'failed_calls': 0,
            'total_tokens_used': 0,
            'total_processing_time': 0
        }
    
    def process_log_chunk(self, chunk: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single chunk of logs through Claude API
        This function is designed to be called by multiprocessing workers
        """
        start_time = time.time()
        
        try:
            if not self.client:
                raise Exception("Anthropic client not available")
                
            # Prepare the log content for Claude
            log_content = self._prepare_log_content(chunk)
            
            # Create the prompt for Claude
            prompt = self._create_cleaning_prompt(log_content, chunk['type'])
            
            # Call Claude API
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4000,
                temperature=0.1,  # Low temperature for consistent cleaning
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Parse Claude's response
            cleaned_logs = self._parse_claude_response(response.content[0].text)
            
            processing_time = time.time() - start_time
            
            # Update stats (note: in multiprocessing, this won't update the main instance)
            result = {
                'chunk_id': chunk.get('chunk_id', 0),
                'cleaned_logs': cleaned_logs,
                'original_line_count': chunk['line_count'],
                'cleaned_line_count': len(cleaned_logs),
                'processing_time_seconds': processing_time,
                'tokens_used': response.usage.input_tokens + response.usage.output_tokens,
                'success': True,
                'error': None
            }
            
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            
            return {
                'chunk_id': chunk.get('chunk_id', 0),
                'cleaned_logs': [],
                'original_line_count': chunk['line_count'],
                'cleaned_line_count': 0,
                'processing_time_seconds': processing_time,
                'tokens_used': 0,
                'success': False,
                'error': str(e)
            }
    
    def _prepare_log_content(self, chunk: Dict[str, Any]) -> str:
        """Prepare log content for Claude processing"""
        if chunk['type'] == 'jsonl':
            return '\n'.join(chunk['content'])
        elif chunk['type'] == 'json':
            return '\n'.join(chunk['content'])
        else:  # plain text
            return '\n'.join(chunk['content'])
    
    def _create_cleaning_prompt(self, log_content: str, log_type: str) -> str:
        """Create Claude prompt for log cleaning and normalization"""
        
        if log_type in ['jsonl', 'json']:
            prompt = f"""You are a log processing expert. Clean and normalize the following log data.

TASK: Process these logs and return clean, structured JSON entries.

REQUIREMENTS:
1. DEDUPLICATE: Remove duplicate entries (same timestamp + message/event)
2. FILL MISSING FIELDS: Add standard fields where missing:
   - timestamp (use ISO format, infer if missing)
   - level (ERROR, WARN, INFO, DEBUG)
   - message (extract/clean the main log message)
   - service (infer from context or use "unknown")
   - user_id (if available, otherwise null)
   - request_id (if available, otherwise null)
3. NORMALIZE: Ensure consistent formatting
4. VALIDATE: Only include valid, complete log entries
5. OUTPUT: Return a JSON array of cleaned log objects

INPUT LOGS:
{log_content}

OUTPUT: Return ONLY the JSON array, no other text or explanation."""

        else:  # plain text logs
            prompt = f"""You are a log processing expert. Parse and normalize the following plain text logs.

TASK: Convert these text logs into clean, structured JSON entries.

REQUIREMENTS:
1. PARSE: Extract structured data from each log line
2. DEDUPLICATE: Remove duplicate entries
3. STANDARDIZE: Create consistent JSON objects with fields:
   - timestamp (ISO format)
   - level (ERROR, WARN, INFO, DEBUG)
   - message (main log content)
   - service (infer from log content)
   - source_ip (if available)
   - user_id (if available)
   - request_id (if available)
4. CLEAN: Remove malformed or incomplete entries
5. OUTPUT: Return a JSON array of log objects

INPUT LOGS:
{log_content}

OUTPUT: Return ONLY the JSON array, no other text or explanation."""

        return prompt
    
    def _parse_claude_response(self, response_text: str) -> List[Dict[str, Any]]:
        """Parse Claude's response into structured log entries"""
        try:
            # Claude should return a JSON array
            # Clean up the response (remove any markdown formatting)
            clean_response = response_text.strip()
            if clean_response.startswith('```json'):
                clean_response = clean_response[7:]  # Remove ```json
            if clean_response.endswith('```'):
                clean_response = clean_response[:-3]  # Remove ```
            
            clean_response = clean_response.strip()
            
            # Parse JSON
            parsed_logs = json.loads(clean_response)
            
            # Ensure it's a list
            if not isinstance(parsed_logs, list):
                parsed_logs = [parsed_logs]
            
            # Validate each log entry
            validated_logs = []
            for log_entry in parsed_logs:
                if self._validate_log_entry(log_entry):
                    validated_logs.append(log_entry)
            
            return validated_logs
            
        except json.JSONDecodeError as e:
            print(f"❌ Failed to parse Claude response as JSON: {e}")
            print(f"Response was: {response_text[:200]}...")
            return []
        except Exception as e:
            print(f"❌ Error processing Claude response: {e}")
            return []
    
    def _validate_log_entry(self, log_entry: Dict[str, Any]) -> bool:
        """Validate that a log entry has required fields"""
        required_fields = ['timestamp', 'level', 'message']
        
        if not isinstance(log_entry, dict):
            return False
        
        for field in required_fields:
            if field not in log_entry or not log_entry[field]:
                return False
        
        # Validate log level
        valid_levels = ['ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE', 'FATAL']
        if log_entry['level'].upper() not in valid_levels:
            return False
        
        return True
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        return {
            'total_api_calls': self.stats['total_api_calls'],
            'successful_calls': self.stats['successful_calls'],
            'failed_calls': self.stats['failed_calls'],
            'success_rate': (self.stats['successful_calls'] / max(1, self.stats['total_api_calls'])) * 100,
            'total_tokens_used': self.stats['total_tokens_used'],
            'total_processing_time': self.stats['total_processing_time'],
            'avg_tokens_per_call': self.stats['total_tokens_used'] / max(1, self.stats['successful_calls']),
            'avg_time_per_call': self.stats['total_processing_time'] / max(1, self.stats['successful_calls'])
        }

# Function for multiprocessing (needs to be at module level)
def process_chunk_with_claude(chunk: Dict[str, Any]) -> Dict[str, Any]:
    """
    Wrapper function for multiprocessing
    Each worker process will create its own Claude client
    """
    try:
        # Create a new Claude processor for this worker
        processor = ClaudeLogProcessor()
        return processor.process_log_chunk(chunk)
    except Exception as e:
        return {
            'chunk_id': chunk.get('chunk_id', 0),
            'cleaned_logs': [],
            'original_line_count': chunk.get('line_count', 0),
            'cleaned_line_count': 0,
            'processing_time_seconds': 0,
            'tokens_used': 0,
            'success': False,
            'error': f"Worker initialization error: {str(e)}"
        }

# Global processor instance (for main process)
try:
    claude_processor = ClaudeLogProcessor() if os.getenv("ANTHROPIC_API_KEY") else None
except Exception as e:
    print(f"Warning: Could not initialize Claude processor: {e}")
    claude_processor = None
