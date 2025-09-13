from fastapi import APIRouter, HTTPException, UploadFile, File, Query, BackgroundTasks
from typing import List, Optional, Dict, Any
import aiofiles
import os
import tempfile
import logging
from datetime import datetime

from app.models.schemas import (
    ProcessingResult, AnalysisRequest, PerformanceMetrics, 
    HealthStatus, LogFile
)
from app.services.distributed_processor import DistributedLogProcessor
from app.core.config import settings


logger = logging.getLogger(__name__)
router = APIRouter()

# Global processor instance
processor = DistributedLogProcessor()


@router.get("/health", response_model=HealthStatus)
async def health_check():
    """Check API health and Ray cluster status."""
    ray_status = processor.get_ray_status()
    
    return HealthStatus(
        status="healthy",
        timestamp=datetime.now(),
        ray_cluster_status=ray_status.get('status', 'unknown'),
        version=settings.app_version
    )


@router.post("/upload", response_model=LogFile)
async def upload_log_file(file: UploadFile = File(...)):
    """Upload a log file for processing."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")
    
    # Check file size
    contents = await file.read()
    if len(contents) > settings.max_file_size:
        raise HTTPException(
            status_code=413, 
            detail=f"File size exceeds maximum allowed size of {settings.max_file_size} bytes"
        )
    
    # Save file to temporary location
    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, f"upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file.filename}")
    
    async with aiofiles.open(file_path, 'wb') as f:
        await f.write(contents)
    
    return LogFile(
        filename=file.filename,
        size=len(contents),
        upload_time=datetime.now(),
        processed=False
    )


@router.post("/analyze", response_model=ProcessingResult)
async def analyze_logs(request: AnalysisRequest):
    """Analyze log file for anomalies."""
    if not os.path.exists(request.file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    try:
        if request.parallel:
            result = processor.process_file_parallel(
                file_path=request.file_path,
                chunk_size=request.chunk_size or settings.chunk_size,
                anomaly_threshold=request.anomaly_threshold or settings.anomaly_threshold
            )
        else:
            result = processor.process_file_sequential(
                file_path=request.file_path,
                chunk_size=request.chunk_size or settings.chunk_size,
                anomaly_threshold=request.anomaly_threshold or settings.anomaly_threshold
            )
        
        return result
    
    except Exception as e:
        logger.error(f"Error analyzing logs: {e}")
        raise HTTPException(status_code=500, detail=f"Error analyzing logs: {str(e)}")


@router.post("/compare-performance")
async def compare_performance(request: AnalysisRequest):
    """Compare parallel vs sequential processing performance."""
    if not os.path.exists(request.file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    try:
        comparison = processor.compare_performance(
            file_path=request.file_path,
            chunk_size=request.chunk_size or settings.chunk_size,
            anomaly_threshold=request.anomaly_threshold or settings.anomaly_threshold
        )
        
        return comparison
    
    except Exception as e:
        logger.error(f"Error in performance comparison: {e}")
        raise HTTPException(status_code=500, detail=f"Error in performance comparison: {str(e)}")


@router.get("/ray/status")
async def get_ray_status():
    """Get detailed Ray cluster status."""
    return processor.get_ray_status()


@router.post("/ray/initialize")
async def initialize_ray():
    """Initialize Ray cluster."""
    success = processor.initialize_ray()
    if success:
        return {"message": "Ray initialized successfully", "status": processor.get_ray_status()}
    else:
        raise HTTPException(status_code=500, detail="Failed to initialize Ray cluster")


@router.post("/ray/shutdown")
async def shutdown_ray():
    """Shutdown Ray cluster."""
    processor.shutdown_ray()
    return {"message": "Ray cluster shutdown successfully"}


@router.get("/sample-analysis")
async def run_sample_analysis():
    """Run analysis on sample log data for demonstration."""
    # Create sample log data
    sample_data = [
        "2024-01-15 10:00:01 INFO Application started successfully",
        "2024-01-15 10:00:02 INFO User login: user123",
        "2024-01-15 10:00:03 INFO Processing request for /api/data",
        "2024-01-15 10:00:04 ERROR Database connection failed - timeout after 30s",
        "2024-01-15 10:00:05 WARN Retrying database connection",
        "2024-01-15 10:00:06 INFO Database connection restored",
        "2024-01-15 10:00:07 INFO User logout: user123",
        "2024-01-15 10:00:08 CRITICAL SYSTEM FAILURE - IMMEDIATE ATTENTION REQUIRED",
        "2024-01-15 10:00:09 ERROR Multiple authentication failures detected",
        "2024-01-15 10:00:10 INFO System recovery initiated",
    ]
    
    # Save sample data to temporary file
    temp_dir = tempfile.gettempdir()
    sample_file = os.path.join(temp_dir, f"sample_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    with open(sample_file, 'w') as f:
        f.write('\n'.join(sample_data))
    
    try:
        # Run analysis
        request = AnalysisRequest(
            file_path=sample_file,
            parallel=True,
            chunk_size=5
        )
        
        result = processor.process_file_parallel(
            file_path=sample_file,
            chunk_size=5,
            anomaly_threshold=1.5  # Lower threshold for demo
        )
        
        # Clean up
        os.remove(sample_file)
        
        return {
            "message": "Sample analysis completed",
            "sample_data": sample_data,
            "result": result
        }
    
    except Exception as e:
        # Clean up on error
        if os.path.exists(sample_file):
            os.remove(sample_file)
        logger.error(f"Error in sample analysis: {e}")
        raise HTTPException(status_code=500, detail=f"Error in sample analysis: {str(e)}")


@router.get("/metrics/performance")
async def get_performance_metrics():
    """Get current performance metrics."""
    ray_status = processor.get_ray_status()
    
    return {
        "ray_cluster": ray_status,
        "configuration": {
            "chunk_size": settings.chunk_size,
            "anomaly_threshold": settings.anomaly_threshold,
            "max_file_size_mb": settings.max_file_size // (1024 * 1024),
            "ray_num_cpus": settings.ray_num_cpus
        },
        "capabilities": {
            "parallel_processing": processor.is_ray_initialized,
            "anomaly_detection_methods": [
                "statistical", "pattern", "temporal", "content"
            ],
            "supported_log_formats": [
                "apache", "nginx", "syslog", "json", "generic"
            ]
        }
    }