import re
import pandas as pd
from datetime import datetime
from typing import List, Iterator, Dict, Any
from app.models.schemas import LogEntry


class LogParser:
    """Log parser for various log formats."""
    
    # Common log patterns
    PATTERNS = {
        'apache': r'(?P<ip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] "(?P<method>\S+) (?P<url>\S+) (?P<protocol>[^"]+)" (?P<status>\d+) (?P<size>\S+)',
        'nginx': r'(?P<ip>\S+) - - \[(?P<timestamp>[^\]]+)\] "(?P<method>\S+) (?P<url>\S+) (?P<protocol>[^"]+)" (?P<status>\d+) (?P<size>\d+)',
        'syslog': r'(?P<timestamp>\w+\s+\d+\s+\d+:\d+:\d+) (?P<hostname>\S+) (?P<process>\S+): (?P<message>.*)',
        'json': r'.*',  # JSON logs will be handled separately
        'generic': r'(?P<timestamp>\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)\s+(?P<level>\w+)\s+(?P<message>.*)',
    }
    
    def __init__(self):
        self.compiled_patterns = {
            name: re.compile(pattern) 
            for name, pattern in self.PATTERNS.items()
        }
    
    def detect_format(self, sample_lines: List[str]) -> str:
        """Detect log format from sample lines."""
        for format_name, pattern in self.compiled_patterns.items():
            if format_name == 'json':
                continue
            
            matches = 0
            for line in sample_lines[:10]:  # Check first 10 lines
                if pattern.match(line.strip()):
                    matches += 1
            
            if matches > len(sample_lines) * 0.7:  # 70% match rate
                return format_name
        
        # Check for JSON format
        try:
            import json
            for line in sample_lines[:5]:
                json.loads(line.strip())
            return 'json'
        except:
            pass
            
        return 'generic'
    
    def parse_line(self, line: str, format_type: str) -> LogEntry:
        """Parse a single log line."""
        line = line.strip()
        if not line:
            return None
            
        try:
            if format_type == 'json':
                return self._parse_json_line(line)
            else:
                return self._parse_regex_line(line, format_type)
        except Exception as e:
            # Fallback to generic parsing
            return self._parse_generic_line(line)
    
    def _parse_json_line(self, line: str) -> LogEntry:
        """Parse JSON log line."""
        import json
        data = json.loads(line)
        
        # Extract common fields
        timestamp = self._parse_timestamp(
            data.get('timestamp') or data.get('time') or data.get('@timestamp')
        )
        level = data.get('level', 'INFO').upper()
        message = data.get('message') or data.get('msg') or str(data)
        source = data.get('source') or data.get('logger')
        
        # Store remaining fields as metadata
        metadata = {k: v for k, v in data.items() 
                   if k not in ['timestamp', 'time', '@timestamp', 'level', 'message', 'msg', 'source', 'logger']}
        
        return LogEntry(
            timestamp=timestamp,
            level=level,
            message=message,
            source=source,
            metadata=metadata
        )
    
    def _parse_regex_line(self, line: str, format_type: str) -> LogEntry:
        """Parse log line using regex pattern."""
        pattern = self.compiled_patterns[format_type]
        match = pattern.match(line)
        
        if not match:
            return self._parse_generic_line(line)
        
        groups = match.groupdict()
        
        timestamp = self._parse_timestamp(groups.get('timestamp'))
        level = groups.get('level', 'INFO').upper()
        message = groups.get('message', line)
        source = groups.get('hostname') or groups.get('process')
        
        # Store additional fields as metadata
        metadata = {k: v for k, v in groups.items() 
                   if k not in ['timestamp', 'level', 'message', 'hostname', 'process']}
        
        return LogEntry(
            timestamp=timestamp,
            level=level,
            message=message,
            source=source,
            metadata=metadata
        )
    
    def _parse_generic_line(self, line: str) -> LogEntry:
        """Fallback generic parsing."""
        # Try to extract timestamp from beginning of line
        timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2})', line)
        timestamp = datetime.now()
        
        if timestamp_match:
            timestamp = self._parse_timestamp(timestamp_match.group(1))
        
        # Try to extract log level
        level_match = re.search(r'\b(DEBUG|INFO|WARN|WARNING|ERROR|FATAL|TRACE)\b', line, re.IGNORECASE)
        level = level_match.group(1).upper() if level_match else 'INFO'
        
        return LogEntry(
            timestamp=timestamp,
            level=level,
            message=line,
            source=None,
            metadata={}
        )
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string to datetime object."""
        if not timestamp_str:
            return datetime.now()
        
        # Common timestamp formats
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%d/%b/%Y:%H:%M:%S',
            '%b %d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(timestamp_str.strip(), fmt)
            except ValueError:
                continue
        
        # If no format matches, return current time
        return datetime.now()
    
    def parse_file(self, file_path: str, chunk_size: int = 1000) -> Iterator[List[LogEntry]]:
        """Parse log file in chunks."""
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            # Read first few lines to detect format
            pos = file.tell()
            sample_lines = [file.readline().strip() for _ in range(10)]
            file.seek(pos)
            
            format_type = self.detect_format([line for line in sample_lines if line])
            
            chunk = []
            for line in file:
                entry = self.parse_line(line, format_type)
                if entry:
                    chunk.append(entry)
                
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
            
            # Yield remaining entries
            if chunk:
                yield chunk