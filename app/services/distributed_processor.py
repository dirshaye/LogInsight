import ray
import time
import logging
from typing import List, Dict, Any, Optional
from concurrent.futures import as_completed
import psutil
import os

from app.models.schemas import LogEntry, AnomalyResult, ProcessingResult, PerformanceMetrics
from app.services.log_parser import LogParser
from app.services.anomaly_detector import AnomalyDetector
from app.core.config import settings


logger = logging.getLogger(__name__)


@ray.remote
class LogProcessorWorker:
    """Ray remote worker for processing log chunks."""
    
    def __init__(self, anomaly_threshold: float = 2.0):
        self.parser = LogParser()
        self.detector = AnomalyDetector(threshold=anomaly_threshold)
    
    def process_chunk(self, chunk_data: List[str], chunk_id: int) -> Dict[str, Any]:
        """Process a chunk of log lines."""
        start_time = time.time()
        
        # Parse log lines
        log_entries = []
        for line in chunk_data:
            if line.strip():
                entry = self.parser.parse_line(line, 'generic')
                if entry:
                    log_entries.append(entry)
        
        # Detect anomalies
        anomaly_results = self.detector.detect_anomalies(log_entries)
        
        processing_time = time.time() - start_time
        
        return {
            'chunk_id': chunk_id,
            'total_entries': len(log_entries),
            'anomalies': [result.dict() for result in anomaly_results],
            'processing_time': processing_time,
            'anomalies_count': sum(1 for r in anomaly_results if r.is_anomaly)
        }


class DistributedLogProcessor:
    """Distributed log processing using Ray."""
    
    def __init__(self):
        self.is_ray_initialized = False
        self.workers = []
        self.parser = LogParser()
        self.detector = AnomalyDetector()
    
    def initialize_ray(self) -> bool:
        """Initialize Ray cluster."""
        try:
            if not ray.is_initialized():
                if settings.ray_cluster_address:
                    ray.init(address=settings.ray_cluster_address)
                else:
                    ray.init(
                        num_cpus=settings.ray_num_cpus,
                        object_store_memory=settings.ray_memory_limit * 1024 * 1024,
                        ignore_reinit_error=True
                    )
                logger.info("Ray initialized successfully")
            self.is_ray_initialized = True
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Ray: {e}")
            self.is_ray_initialized = False
            return False
    
    def shutdown_ray(self):
        """Shutdown Ray cluster."""
        try:
            if ray.is_initialized():
                ray.shutdown()
                logger.info("Ray shutdown successfully")
        except Exception as e:
            logger.error(f"Error shutting down Ray: {e}")
        finally:
            self.is_ray_initialized = False
    
    def get_ray_status(self) -> Dict[str, Any]:
        """Get Ray cluster status."""
        if not ray.is_initialized():
            return {'status': 'not_initialized', 'nodes': 0, 'cpus': 0, 'memory': 0}
        
        try:
            cluster_resources = ray.cluster_resources()
            return {
                'status': 'connected',
                'nodes': len(ray.nodes()),
                'cpus': cluster_resources.get('CPU', 0),
                'memory': cluster_resources.get('memory', 0),
                'object_store_memory': cluster_resources.get('object_store_memory', 0)
            }
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def process_file_parallel(
        self, 
        file_path: str, 
        chunk_size: int = 1000,
        anomaly_threshold: float = 2.0
    ) -> ProcessingResult:
        """Process log file using distributed Ray workers."""
        if not self.is_ray_initialized:
            if not self.initialize_ray():
                # Fallback to sequential processing
                return self.process_file_sequential(file_path, chunk_size, anomaly_threshold)
        
        start_time = time.time()
        total_entries = 0
        all_anomalies = []
        
        # Get system metrics before processing
        process = psutil.Process()
        cpu_before = process.cpu_percent()
        memory_before = process.memory_info().rss / 1024 / 1024  # MB
        
        try:
            # Read file and split into chunks
            chunks = self._read_file_chunks(file_path, chunk_size)
            
            # Create worker pool
            num_workers = min(len(chunks), settings.ray_num_cpus)
            workers = [LogProcessorWorker.remote(anomaly_threshold) for _ in range(num_workers)]
            
            # Submit tasks
            futures = []
            for i, chunk in enumerate(chunks):
                worker = workers[i % num_workers]
                future = worker.process_chunk.remote(chunk, i)
                futures.append(future)
            
            # Collect results
            for future in futures:
                try:
                    result = ray.get(future, timeout=60)  # 60 second timeout per chunk
                    total_entries += result['total_entries']
                    
                    # Convert anomaly results back to objects
                    for anomaly_data in result['anomalies']:
                        anomaly = AnomalyResult(**anomaly_data)
                        all_anomalies.append(anomaly)
                
                except Exception as e:
                    logger.error(f"Error processing chunk: {e}")
                    continue
            
            # Clean up workers
            for worker in workers:
                ray.kill(worker)
        
        except Exception as e:
            logger.error(f"Error in parallel processing: {e}")
            # Fallback to sequential processing
            return self.process_file_sequential(file_path, chunk_size, anomaly_threshold)
        
        processing_time = time.time() - start_time
        
        # Get system metrics after processing
        cpu_after = process.cpu_percent()
        memory_after = process.memory_info().rss / 1024 / 1024  # MB
        
        # Calculate performance metrics
        throughput = total_entries / processing_time if processing_time > 0 else 0
        
        performance_metrics = {
            'cpu_usage_before': cpu_before,
            'cpu_usage_after': cpu_after,
            'memory_usage_before_mb': memory_before,
            'memory_usage_after_mb': memory_after,
            'throughput_entries_per_second': throughput,
            'parallel_workers_used': num_workers,
            'chunks_processed': len(chunks)
        }
        
        return ProcessingResult(
            file_id=os.path.basename(file_path),
            total_entries=total_entries,
            processing_time_seconds=processing_time,
            anomalies_detected=sum(1 for a in all_anomalies if a.is_anomaly),
            anomalies=all_anomalies,
            performance_metrics=performance_metrics
        )
    
    def process_file_sequential(
        self, 
        file_path: str, 
        chunk_size: int = 1000,
        anomaly_threshold: float = 2.0
    ) -> ProcessingResult:
        """Process log file sequentially for comparison."""
        start_time = time.time()
        total_entries = 0
        all_anomalies = []
        
        # Get system metrics before processing
        process = psutil.Process()
        cpu_before = process.cpu_percent()
        memory_before = process.memory_info().rss / 1024 / 1024  # MB
        
        try:
            # Process file chunk by chunk
            for chunk in self.parser.parse_file(file_path, chunk_size):
                total_entries += len(chunk)
                anomaly_results = self.detector.detect_anomalies(chunk)
                all_anomalies.extend(anomaly_results)
        
        except Exception as e:
            logger.error(f"Error in sequential processing: {e}")
            raise
        
        processing_time = time.time() - start_time
        
        # Get system metrics after processing
        cpu_after = process.cpu_percent()
        memory_after = process.memory_info().rss / 1024 / 1024  # MB
        
        # Calculate performance metrics
        throughput = total_entries / processing_time if processing_time > 0 else 0
        
        performance_metrics = {
            'cpu_usage_before': cpu_before,
            'cpu_usage_after': cpu_after,
            'memory_usage_before_mb': memory_before,
            'memory_usage_after_mb': memory_after,
            'throughput_entries_per_second': throughput,
            'sequential_processing': True
        }
        
        return ProcessingResult(
            file_id=os.path.basename(file_path),
            total_entries=total_entries,
            processing_time_seconds=processing_time,
            anomalies_detected=sum(1 for a in all_anomalies if a.is_anomaly),
            anomalies=all_anomalies,
            performance_metrics=performance_metrics
        )
    
    def compare_performance(
        self,
        file_path: str,
        chunk_size: int = 1000,
        anomaly_threshold: float = 2.0
    ) -> Dict[str, Any]:
        """Compare parallel vs sequential processing performance."""
        # Process sequentially
        logger.info("Starting sequential processing...")
        sequential_result = self.process_file_sequential(file_path, chunk_size, anomaly_threshold)
        
        # Process in parallel
        logger.info("Starting parallel processing...")
        parallel_result = self.process_file_parallel(file_path, chunk_size, anomaly_threshold)
        
        # Calculate speedup
        speedup = (sequential_result.processing_time_seconds / 
                  parallel_result.processing_time_seconds) if parallel_result.processing_time_seconds > 0 else 1.0
        
        return {
            'sequential_result': sequential_result,
            'parallel_result': parallel_result,
            'speedup_factor': speedup,
            'performance_improvement': (speedup - 1) * 100,  # percentage improvement
            'comparison_summary': {
                'sequential_time': sequential_result.processing_time_seconds,
                'parallel_time': parallel_result.processing_time_seconds,
                'time_saved_seconds': sequential_result.processing_time_seconds - parallel_result.processing_time_seconds,
                'speedup': speedup
            }
        }
    
    def _read_file_chunks(self, file_path: str, chunk_size: int) -> List[List[str]]:
        """Read file and split into chunks for parallel processing."""
        chunks = []
        current_chunk = []
        
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            for line in file:
                current_chunk.append(line.strip())
                
                if len(current_chunk) >= chunk_size:
                    chunks.append(current_chunk)
                    current_chunk = []
        
        # Add remaining lines as final chunk
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks