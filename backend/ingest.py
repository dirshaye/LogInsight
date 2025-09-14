"""
Log Ingestion Module - LogInsight Agent
Handles file upload and initial processing of raw log files
"""
import os
import tempfile
from typing import List, BinaryIO, TextIO
from fastapi import UploadFile, HTTPException
import json
from datetime import datetime

class LogIngestor:
    """Handles ingestion of raw log files (JSONL, plain text)"""
    
    def __init__(self, max_file_size: int = 500 * 1024 * 1024):  # 500MB max
        self.max_file_size = max_file_size
        self.temp_dir = tempfile.mkdtemp(prefix="loginsight_")
        self.uploaded_files = {}  # Track uploaded files
        
    async def upload_log_file(self, file: UploadFile) -> dict:
        """
        Upload and validate a log file
        Returns file info for further processing
        """
        # Validate file
        if not file.filename:
            raise HTTPException(status_code=400, detail="No file provided")
            
        # Check file size
        file_content = await file.read()
        if len(file_content) > self.max_file_size:
            raise HTTPException(
                status_code=413, 
                detail=f"File too large. Max size: {self.max_file_size / (1024*1024):.0f}MB"
            )
        
        # Detect file type
        file_type = self._detect_file_type(file.filename, file_content)
        
        # Save to temporary location
        file_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}"
        temp_path = os.path.join(self.temp_dir, file_id)
        
        with open(temp_path, 'wb') as f:
            f.write(file_content)
        
        # Store file info
        file_info = {
            'file_id': file_id,
            'original_name': file.filename,
            'file_type': file_type,
            'size_bytes': len(file_content),
            'temp_path': temp_path,
            'upload_time': datetime.now().isoformat(),
            'status': 'uploaded'
        }
        
        self.uploaded_files[file_id] = file_info
        
        return file_info
    
    def _detect_file_type(self, filename: str, content: bytes) -> str:
        """Detect if file is JSONL, JSON, or plain text"""
        
        # Check extension first
        if filename.lower().endswith('.jsonl'):
            return 'jsonl'
        elif filename.lower().endswith('.json'):
            return 'json'
        elif filename.lower().endswith('.log'):
            return 'plain_text'
        
        # Try to parse content
        try:
            content_str = content.decode('utf-8')[:1000]  # First 1KB
            
            # Check if it's JSON array
            if content_str.strip().startswith('['):
                return 'json'
            
            # Check if it's JSONL (each line is JSON)
            lines = content_str.split('\n')[:5]  # First 5 lines
            json_line_count = 0
            for line in lines:
                if line.strip():
                    try:
                        json.loads(line.strip())
                        json_line_count += 1
                    except:
                        break
            
            if json_line_count >= 2:  # At least 2 valid JSON lines
                return 'jsonl'
            
            return 'plain_text'
            
        except:
            return 'plain_text'
    
    def get_file_info(self, file_id: str) -> dict:
        """Get information about an uploaded file"""
        if file_id not in self.uploaded_files:
            raise HTTPException(status_code=404, detail="File not found")
        return self.uploaded_files[file_id]
    
    def list_uploaded_files(self) -> List[dict]:
        """List all uploaded files"""
        return list(self.uploaded_files.values())
    
    def read_file_content(self, file_id: str) -> str:
        """Read the content of an uploaded file"""
        file_info = self.get_file_info(file_id)
        
        with open(file_info['temp_path'], 'r', encoding='utf-8') as f:
            return f.read()
    
    def cleanup_temp_files(self):
        """Clean up temporary files"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

# Global ingestor instance
log_ingestor = LogIngestor()
