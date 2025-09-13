from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class LogEntry(BaseModel):
    """Individual log entry model."""
    timestamp: datetime
    level: str
    message: str
    source: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = {}


class LogFile(BaseModel):
    """Log file information model."""
    filename: str
    size: int
    upload_time: datetime
    total_entries: Optional[int] = None
    processed: bool = False


class AnomalyResult(BaseModel):
    """Anomaly detection result model."""
    log_entry: LogEntry
    anomaly_score: float
    is_anomaly: bool
    detection_method: str
    explanation: Optional[str] = None


class ProcessingResult(BaseModel):
    """Log processing result model."""
    file_id: str
    total_entries: int
    processing_time_seconds: float
    anomalies_detected: int
    anomalies: List[AnomalyResult]
    performance_metrics: Dict[str, float]


class AnalysisRequest(BaseModel):
    """Request model for log analysis."""
    file_path: str
    anomaly_threshold: Optional[float] = 2.0
    chunk_size: Optional[int] = 1000
    parallel: bool = True


class PerformanceMetrics(BaseModel):
    """Performance metrics model."""
    total_processing_time: float
    parallel_processing_time: Optional[float] = None
    sequential_processing_time: Optional[float] = None
    speedup_factor: Optional[float] = None
    cpu_usage: Optional[float] = None
    memory_usage: Optional[float] = None
    throughput_entries_per_second: float


class HealthStatus(BaseModel):
    """API health status model."""
    status: str
    timestamp: datetime
    ray_cluster_status: str
    version: str