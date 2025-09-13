import os
from typing import Optional
try:
    from pydantic import BaseSettings
except ImportError:
    from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    app_name: str = "ParalogX - Parallel Log Analyzer"
    app_version: str = "1.0.0"
    debug: bool = False
    
    # Ray configuration
    ray_cluster_address: Optional[str] = None
    ray_num_cpus: int = 4
    ray_memory_limit: int = 2000  # MB
    
    # Log processing configuration
    chunk_size: int = 1000
    anomaly_threshold: float = 2.0
    max_file_size: int = 100 * 1024 * 1024  # 100MB
    
    # API configuration
    api_prefix: str = "/api/v1"
    cors_origins: list = ["*"]
    
    class Config:
        env_file = ".env"


settings = Settings()