#!/usr/bin/env python3
"""
Simple demonstration of ParalogX functionality without external dependencies.
This shows the core concepts of parallel log processing and anomaly detection.
"""

import json
import time
import threading
import queue
import re
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed


class LogEntry:
    """Simple log entry representation."""
    def __init__(self, timestamp: datetime, level: str, message: str, source: str = None):
        self.timestamp = timestamp
        self.level = level
        self.message = message
        self.source = source
        self.metadata = {}

    def to_dict(self):
        return {
            'timestamp': self.timestamp.isoformat(),
            'level': self.level,
            'message': self.message,
            'source': self.source,
            'metadata': self.metadata
        }


class AnomalyResult:
    """Anomaly detection result."""
    def __init__(self, log_entry: LogEntry, anomaly_score: float, is_anomaly: bool, method: str):
        self.log_entry = log_entry
        self.anomaly_score = anomaly_score
        self.is_anomaly = is_anomaly
        self.detection_method = method
        
    def to_dict(self):
        return {
            'log_entry': self.log_entry.to_dict(),
            'anomaly_score': self.anomaly_score,
            'is_anomaly': self.is_anomaly,
            'detection_method': self.detection_method
        }


class SimpleLogParser:
    """Simple log parser for demonstration."""
    
    def parse_line(self, line: str) -> Optional[LogEntry]:
        """Parse a single log line."""
        line = line.strip()
        if not line:
            return None
            
        # Try to parse timestamp
        timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2})', line)
        timestamp = datetime.now()
        if timestamp_match:
            try:
                timestamp = datetime.strptime(timestamp_match.group(1), '%Y-%m-%d %H:%M:%S')
            except:
                pass
        
        # Try to extract log level
        level_match = re.search(r'\b(DEBUG|INFO|WARN|WARNING|ERROR|FATAL|CRITICAL)\b', line, re.IGNORECASE)
        level = level_match.group(1).upper() if level_match else 'INFO'
        
        return LogEntry(timestamp=timestamp, level=level, message=line)
    
    def parse_file(self, file_path: str, chunk_size: int = 100) -> List[List[LogEntry]]:
        """Parse file into chunks."""
        chunks = []
        current_chunk = []
        
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            for line in file:
                entry = self.parse_line(line)
                if entry:
                    current_chunk.append(entry)
                
                if len(current_chunk) >= chunk_size:
                    chunks.append(current_chunk)
                    current_chunk = []
        
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks


class SimpleAnomalyDetector:
    """Simple anomaly detector for demonstration."""
    
    def __init__(self, threshold: float = 2.0):
        self.threshold = threshold
        self.error_keywords = ['error', 'exception', 'fail', 'crash', 'timeout', 'critical', 'fatal']
    
    def detect_anomalies(self, log_entries: List[LogEntry]) -> List[AnomalyResult]:
        """Detect anomalies in log entries."""
        results = []
        
        if not log_entries:
            return results
        
        # Calculate message length statistics
        message_lengths = [len(entry.message) for entry in log_entries]
        avg_length = sum(message_lengths) / len(message_lengths)
        
        # Calculate level distribution
        level_counts = {}
        for entry in log_entries:
            level_counts[entry.level] = level_counts.get(entry.level, 0) + 1
        
        total_entries = len(log_entries)
        
        for entry in log_entries:
            anomaly_score = 0.0
            methods = []
            
            # Check message length anomaly
            if len(entry.message) > avg_length * 2:
                anomaly_score += 1.0
                methods.append('length')
            
            # Check error keywords
            error_count = sum(1 for keyword in self.error_keywords 
                            if keyword.lower() in entry.message.lower())
            if error_count > 0:
                anomaly_score += error_count * 0.5
                methods.append('keywords')
            
            # Check rare log levels
            level_frequency = level_counts[entry.level] / total_entries
            if level_frequency < 0.1 and entry.level in ['ERROR', 'CRITICAL', 'FATAL']:
                anomaly_score += 2.0
                methods.append('rare_level')
            
            # Check for suspicious patterns
            if re.search(r'(\d{1,3}\.){3}\d{1,3}', entry.message):  # IP addresses
                if any(word in entry.message.lower() for word in ['attack', 'intrusion', 'hack']):
                    anomaly_score += 1.5
                    methods.append('security')
            
            is_anomaly = anomaly_score >= self.threshold
            
            if is_anomaly or anomaly_score > 0:
                results.append(AnomalyResult(
                    log_entry=entry,
                    anomaly_score=anomaly_score,
                    is_anomaly=is_anomaly,
                    method=','.join(methods)
                ))
        
        return results


def process_chunk_worker(chunk_id: int, chunk: List[LogEntry], threshold: float) -> Dict[str, Any]:
    """Worker function to process a chunk of log entries."""
    start_time = time.time()
    
    detector = SimpleAnomalyDetector(threshold)
    anomaly_results = detector.detect_anomalies(chunk)
    
    processing_time = time.time() - start_time
    
    return {
        'chunk_id': chunk_id,
        'total_entries': len(chunk),
        'anomalies': [result.to_dict() for result in anomaly_results],
        'processing_time': processing_time,
        'anomalies_count': sum(1 for r in anomaly_results if r.is_anomaly)
    }


class SimpleDistributedProcessor:
    """Simple distributed processor using ThreadPoolExecutor."""
    
    def __init__(self):
        self.parser = SimpleLogParser()
    
    def process_file_sequential(self, file_path: str, chunk_size: int = 100, threshold: float = 2.0) -> Dict[str, Any]:
        """Process file sequentially."""
        start_time = time.time()
        
        chunks = self.parser.parse_file(file_path, chunk_size)
        all_results = []
        total_entries = 0
        
        for i, chunk in enumerate(chunks):
            result = process_chunk_worker(i, chunk, threshold)
            all_results.extend(result['anomalies'])
            total_entries += result['total_entries']
        
        processing_time = time.time() - start_time
        
        return {
            'method': 'sequential',
            'total_entries': total_entries,
            'processing_time': processing_time,
            'anomalies_detected': sum(1 for r in all_results if r.get('is_anomaly')),
            'anomalies': all_results,
            'chunks_processed': len(chunks)
        }
    
    def process_file_parallel(self, file_path: str, chunk_size: int = 100, threshold: float = 2.0, max_workers: int = 4) -> Dict[str, Any]:
        """Process file in parallel using threads."""
        start_time = time.time()
        
        chunks = self.parser.parse_file(file_path, chunk_size)
        all_results = []
        total_entries = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(process_chunk_worker, i, chunk, threshold): i 
                for i, chunk in enumerate(chunks)
            }
            
            # Collect results
            for future in as_completed(futures):
                try:
                    result = future.result()
                    all_results.extend(result['anomalies'])
                    total_entries += result['total_entries']
                except Exception as e:
                    print(f"Error processing chunk: {e}")
        
        processing_time = time.time() - start_time
        
        return {
            'method': 'parallel',
            'total_entries': total_entries,
            'processing_time': processing_time,
            'anomalies_detected': sum(1 for r in all_results if r.get('is_anomaly')),
            'anomalies': all_results,
            'chunks_processed': len(chunks),
            'workers_used': max_workers
        }
    
    def compare_performance(self, file_path: str, chunk_size: int = 100, threshold: float = 2.0) -> Dict[str, Any]:
        """Compare sequential vs parallel processing."""
        print(f"Processing {file_path}...")
        print(f"Chunk size: {chunk_size}, Threshold: {threshold}")
        
        # Sequential processing
        print("\n🔄 Running sequential processing...")
        sequential_result = self.process_file_sequential(file_path, chunk_size, threshold)
        
        # Parallel processing
        print("🚀 Running parallel processing...")
        parallel_result = self.process_file_parallel(file_path, chunk_size, threshold)
        
        # Calculate speedup
        speedup = sequential_result['processing_time'] / parallel_result['processing_time']
        
        return {
            'sequential': sequential_result,
            'parallel': parallel_result,
            'speedup_factor': speedup,
            'performance_improvement': (speedup - 1) * 100,
            'comparison': {
                'sequential_time': sequential_result['processing_time'],
                'parallel_time': parallel_result['processing_time'],
                'time_saved': sequential_result['processing_time'] - parallel_result['processing_time'],
                'speedup': speedup
            }
        }


def create_large_sample_log(file_path: str, num_lines: int = 1000):
    """Create a larger sample log file for testing."""
    import random
    
    log_levels = ['INFO', 'WARN', 'ERROR', 'DEBUG', 'CRITICAL']
    normal_messages = [
        "User login successful",
        "Database query executed successfully",
        "HTTP request processed",
        "Cache hit for key",
        "Session created",
        "API endpoint called",
        "File uploaded successfully",
        "Backup completed",
        "System health check passed",
        "Configuration loaded"
    ]
    
    anomaly_messages = [
        "CRITICAL: System failure detected - immediate attention required",
        "ERROR: Database connection failed after 30 seconds timeout",
        "ERROR: Multiple authentication failures from suspicious IP 10.0.0.666",
        "CRITICAL: Memory usage exceeded 95% - system unstable",
        "ERROR: SQL injection attempt detected in user input",
        "FATAL: Application crashed with OutOfMemoryError",
        "ERROR: Certificate expired - SSL handshake failed",
        "CRITICAL: Disk space critically low - 99% usage detected"
    ]
    
    with open(file_path, 'w') as f:
        for i in range(num_lines):
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 10% chance of anomaly
            if random.random() < 0.1:
                level = random.choice(['ERROR', 'CRITICAL', 'FATAL'])
                message = random.choice(anomaly_messages)
            else:
                level = random.choice(log_levels)
                message = random.choice(normal_messages)
            
            f.write(f"{timestamp} {level} {message}\n")


def main():
    """Main demonstration function."""
    print("🚀 ParalogX - Parallel Log Analyzer Demo")
    print("=" * 50)
    
    # Create sample data
    sample_dir = '/home/runner/work/paralogX/paralogX/sample_data'
    large_sample_path = os.path.join(sample_dir, 'large_sample.log')
    
    if not os.path.exists(large_sample_path):
        print("📝 Creating large sample log file...")
        create_large_sample_log(large_sample_path, 2000)
        print(f"✅ Created {large_sample_path} with 2000 log entries")
    
    # Initialize processor
    processor = SimpleDistributedProcessor()
    
    # Test with existing sample file
    existing_sample = os.path.join(sample_dir, 'application.log')
    if os.path.exists(existing_sample):
        print(f"\n📊 Analyzing {existing_sample}...")
        result = processor.compare_performance(existing_sample, chunk_size=10, threshold=1.5)
        
        print(f"\n📈 Results for {existing_sample}:")
        print(f"Sequential processing: {result['sequential']['processing_time']:.4f}s")
        print(f"Parallel processing: {result['parallel']['processing_time']:.4f}s")
        print(f"Speedup factor: {result['speedup_factor']:.2f}x")
        print(f"Performance improvement: {result['performance_improvement']:.1f}%")
        print(f"Anomalies detected: {result['parallel']['anomalies_detected']}")
    
    # Test with large sample
    print(f"\n📊 Analyzing large sample ({large_sample_path})...")
    result = processor.compare_performance(large_sample_path, chunk_size=100, threshold=2.0)
    
    print(f"\n📈 Results for large sample:")
    print(f"Sequential processing: {result['sequential']['processing_time']:.4f}s")
    print(f"Parallel processing: {result['parallel']['processing_time']:.4f}s")
    print(f"Speedup factor: {result['speedup_factor']:.2f}x")
    print(f"Performance improvement: {result['performance_improvement']:.1f}%")
    print(f"Total entries processed: {result['parallel']['total_entries']}")
    print(f"Anomalies detected: {result['parallel']['anomalies_detected']}")
    print(f"Chunks processed: {result['parallel']['chunks_processed']}")
    print(f"Workers used: {result['parallel']['workers_used']}")
    
    # Show some anomaly examples
    anomalies = result['parallel']['anomalies']
    high_score_anomalies = [a for a in anomalies if a.get('is_anomaly') and a.get('anomaly_score', 0) > 2.0]
    
    if high_score_anomalies:
        print(f"\n🔍 Top anomalies detected:")
        for i, anomaly in enumerate(high_score_anomalies[:5]):
            log_entry = anomaly['log_entry']
            print(f"{i+1}. Score: {anomaly['anomaly_score']:.2f} | {log_entry['level']} | {log_entry['message'][:80]}...")
    
    print(f"\n✅ Demo completed successfully!")
    print("🎯 Key Features Demonstrated:")
    print("   - ⚡ Parallel processing with threading")
    print("   - 🔍 Multi-method anomaly detection")
    print("   - 📊 Performance comparison and metrics")
    print("   - 🔧 Automatic log format detection and parsing")


if __name__ == "__main__":
    main()