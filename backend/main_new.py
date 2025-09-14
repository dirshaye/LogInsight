"""
LogInsight Agent Backend API - Log Processing Pipeline
Focused on ingestion → cleaning → merging pipeline as per instructions
"""
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import json
import os
from datetime import datetime
import asyncio

# Import our modular components
from ingest import log_ingestor
from parallel import log_chunker, parallel_processor
from anthropic_client import process_chunk_with_claude, claude_processor
from merge import log_merger

app = FastAPI(
    title="LogInsight Agent - Log Processing Pipeline",
    description="Modular log processing pipeline with upload → chunk → parallel clean → merge",
    version="3.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Models
class ProcessingRequest(BaseModel):
    file_id: str
    use_parallel: bool = True
    chunk_size_mb: int = 50

class ProcessingResponse(BaseModel):
    processing_id: str
    status: str
    file_info: Dict[str, Any]
    chunk_count: int
    estimated_time_minutes: float

class CleanedLogsResponse(BaseModel):
    logs: List[Dict[str, Any]]
    total_count: int
    statistics: Dict[str, Any]

# Global processing jobs tracker
processing_jobs = {}

@app.on_event("startup")
async def startup_event():
    """Initialize the application"""
    print("🚀 LogInsight Agent Pipeline starting up...")
    print("📁 File ingestion system ready")
    print("⚡ Parallel processing engine ready")
    print("🧠 Claude AI integration ready")
    print("🔄 Log merger and deduplication ready")
    
    # Check if Claude is available
    if claude_processor:
        print("✅ Anthropic Claude API client initialized")
    else:
        print("⚠️ Anthropic API not available - check ANTHROPIC_API_KEY")

@app.get("/")
async def root():
    """Root endpoint with pipeline status"""
    return {
        "message": "LogInsight Agent - Log Processing Pipeline",
        "version": "3.0.0",
        "status": "running",
        "pipeline_flow": [
            "1. Upload raw log files (/upload)",
            "2. Chunk files for parallel processing",
            "3. Clean logs with Claude AI in parallel",
            "4. Merge and deduplicate results",
            "5. Retrieve cleaned logs (/cleaned)"
        ],
        "features": {
            "file_upload": True,
            "chunking": True,
            "parallel_processing": True,
            "ai_cleaning": claude_processor is not None,
            "deduplication": True,
            "benchmarking": True
        },
        "parallel_workers": parallel_processor.max_workers
    }

@app.post("/upload")
async def upload_log_file(file: UploadFile = File(...)):
    """
    Upload raw log files (JSONL, JSON, or plain text)
    Step 1 of the pipeline
    """
    try:
        # Upload and process file
        file_info = await log_ingestor.upload_log_file(file)
        
        return {
            "message": "File uploaded successfully",
            "file_info": file_info,
            "next_step": f"POST /process with file_id: {file_info['file_id']}"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/uploaded-files")
async def list_uploaded_files():
    """List all uploaded files"""
    try:
        files = log_ingestor.list_uploaded_files()
        return {"uploaded_files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process", response_model=ProcessingResponse)
async def process_log_file(request: ProcessingRequest):
    """
    Process uploaded log file through the complete pipeline:
    chunking → parallel processing → merging
    """
    try:
        # Get file info
        file_info = log_ingestor.get_file_info(request.file_id)
        
        # Step 1: Chunk the file
        print(f"📦 Chunking file: {file_info['original_name']}")
        chunks = log_chunker.chunk_file(
            file_info['temp_path'], 
            file_info['file_type']
        )
        
        if not chunks:
            raise HTTPException(status_code=400, detail="No chunks created from file")
        
        print(f"✅ Created {len(chunks)} chunks")
        
        # Step 2: Process chunks in parallel (or sequential for benchmarking)
        if request.use_parallel:
            print("🚀 Starting parallel processing...")
            processing_results = parallel_processor.process_chunks_parallel(
                chunks, process_chunk_with_claude
            )
        else:
            print("🐌 Starting sequential processing...")
            processing_results = parallel_processor.process_chunks_sequential(
                chunks, process_chunk_with_claude
            )
        
        # Step 3: Merge results and deduplicate
        print("🔄 Merging and deduplicating results...")
        merge_stats = log_merger.merge_chunks(processing_results['results'])
        
        # Create processing job record
        processing_id = f"proc_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{request.file_id}"
        processing_jobs[processing_id] = {
            'file_id': request.file_id,
            'file_info': file_info,
            'chunk_count': len(chunks),
            'processing_method': 'parallel' if request.use_parallel else 'sequential',
            'processing_results': processing_results,
            'merge_stats': merge_stats,
            'completed_at': datetime.now().isoformat(),
            'status': 'completed'
        }
        
        return ProcessingResponse(
            processing_id=processing_id,
            status="completed",
            file_info=file_info,
            chunk_count=len(chunks),
            estimated_time_minutes=processing_results['benchmarks']['processing_time_seconds'] / 60
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

@app.get("/cleaned", response_model=CleanedLogsResponse)
async def get_cleaned_logs(
    limit: Optional[int] = 1000,
    level: Optional[str] = None,
    service: Optional[str] = None
):
    """
    Get cleaned and merged logs
    Final step of the pipeline
    """
    try:
        # Get cleaned logs with optional filtering
        cleaned_logs = log_merger.get_cleaned_logs(
            limit=limit,
            level_filter=level,
            service_filter=service
        )
        
        # Get statistics
        statistics = log_merger.get_log_statistics()
        
        return CleanedLogsResponse(
            logs=cleaned_logs,
            total_count=len(cleaned_logs),
            statistics=statistics
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve logs: {str(e)}")

@app.get("/processing-jobs")
async def list_processing_jobs():
    """List all processing jobs"""
    return {"processing_jobs": list(processing_jobs.values())}

@app.get("/processing-jobs/{processing_id}")
async def get_processing_job(processing_id: str):
    """Get details of a specific processing job"""
    if processing_id not in processing_jobs:
        raise HTTPException(status_code=404, detail="Processing job not found")
    
    return {"processing_job": processing_jobs[processing_id]}

@app.post("/benchmark")
async def benchmark_processing():
    """
    Compare parallel vs sequential processing performance
    """
    try:
        # Find a recent file to benchmark with
        files = log_ingestor.list_uploaded_files()
        if not files:
            raise HTTPException(status_code=400, detail="No uploaded files to benchmark with")
        
        # Use the most recent file
        recent_file = max(files, key=lambda x: x['upload_time'])
        
        # Chunk the file
        chunks = log_chunker.chunk_file(
            recent_file['temp_path'], 
            recent_file['file_type']
        )
        
        if not chunks:
            raise HTTPException(status_code=400, detail="No chunks created for benchmarking")
        
        # Run comparison
        comparison_results = parallel_processor.compare_processing_methods(
            chunks, process_chunk_with_claude
        )
        
        return {
            "benchmark_results": comparison_results,
            "file_used": recent_file['original_name'],
            "chunk_count": len(chunks),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Benchmark failed: {str(e)}")

@app.get("/statistics")
async def get_system_statistics():
    """Get overall system statistics"""
    try:
        return {
            "uploaded_files": len(log_ingestor.list_uploaded_files()),
            "processing_jobs": len(processing_jobs),
            "log_statistics": log_merger.get_log_statistics(),
            "claude_stats": claude_processor.get_processing_stats() if claude_processor else None,
            "parallel_workers": parallel_processor.max_workers
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/cleanup")
async def cleanup_system():
    """Clean up temporary files and reset system"""
    try:
        log_ingestor.cleanup_temp_files()
        log_merger.cleanup()
        processing_jobs.clear()
        
        return {"message": "System cleaned up successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "pipeline_components": {
            "ingestor": True,
            "chunker": True,
            "parallel_processor": True,
            "claude_client": claude_processor is not None,
            "merger": True
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
