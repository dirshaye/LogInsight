"""
Parallel Processing Module - LogInsight Agent
Handles chunking and parallel processing of log files
"""
import os
import math
import time
from typing import List, Dict, Any, Callable
from multiprocessing import Pool, cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed
import json
from datetime import datetime

class LogChunker:
    """Splits log files into fixed-size chunks for parallel processing"""
    
    def __init__(self, chunk_size_mb: int = 50):
        self.chunk_size_bytes = chunk_size_mb * 1024 * 1024
        
    def chunk_file(self, file_path: str, file_type: str) -> List[Dict[str, Any]]:
        """
        Split file into chunks based on size and type
        Returns list of chunk metadata
        """
        chunks = []
        
        if file_type == 'jsonl':
            chunks = self._chunk_jsonl(file_path)
        elif file_type == 'json':
            chunks = self._chunk_json(file_path)
        else:  # plain_text
            chunks = self._chunk_text(file_path)
        
        return chunks
    
    def _chunk_jsonl(self, file_path: str) -> List[Dict[str, Any]]:
        """Chunk JSONL file by lines while respecting size limits"""
        chunks = []
        current_chunk = []
        current_size = 0
        chunk_id = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line_size = len(line.encode('utf-8'))
                
                # If adding this line would exceed chunk size, start new chunk
                if current_size + line_size > self.chunk_size_bytes and current_chunk:
                    chunks.append(self._create_chunk_metadata(
                        chunk_id, current_chunk, current_size, 'jsonl'
                    ))
                    current_chunk = []
                    current_size = 0
                    chunk_id += 1
                
                current_chunk.append(line.strip())
                current_size += line_size
        
        # Add final chunk if there's content
        if current_chunk:
            chunks.append(self._create_chunk_metadata(
                chunk_id, current_chunk, current_size, 'jsonl'
            ))
        
        return chunks
    
    def _chunk_json(self, file_path: str) -> List[Dict[str, Any]]:
        """Chunk JSON array by splitting the array"""
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                if not isinstance(data, list):
                    # Convert single object to list
                    data = [data]
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON file")
        
        # Calculate items per chunk based on estimated size
        total_size = len(json.dumps(data).encode('utf-8'))
        items_per_chunk = max(1, len(data) * self.chunk_size_bytes // total_size)
        
        chunks = []
        for i in range(0, len(data), items_per_chunk):
            chunk_data = data[i:i + items_per_chunk]
            chunk_content = [json.dumps(item) for item in chunk_data]
            chunk_size = len(json.dumps(chunk_data).encode('utf-8'))
            
            chunks.append(self._create_chunk_metadata(
                i // items_per_chunk, chunk_content, chunk_size, 'json'
            ))
        
        return chunks
    
    def _chunk_text(self, file_path: str) -> List[Dict[str, Any]]:
        """Chunk plain text file by lines"""
        chunks = []
        current_chunk = []
        current_size = 0
        chunk_id = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line_size = len(line.encode('utf-8'))
                
                if current_size + line_size > self.chunk_size_bytes and current_chunk:
                    chunks.append(self._create_chunk_metadata(
                        chunk_id, current_chunk, current_size, 'text'
                    ))
                    current_chunk = []
                    current_size = 0
                    chunk_id += 1
                
                current_chunk.append(line.strip())
                current_size += line_size
        
        if current_chunk:
            chunks.append(self._create_chunk_metadata(
                chunk_id, current_chunk, current_size, 'text'
            ))
        
        return chunks
    
    def _create_chunk_metadata(self, chunk_id: int, content: List[str], 
                             size: int, chunk_type: str) -> Dict[str, Any]:
        """Create chunk metadata"""
        return {
            'chunk_id': chunk_id,
            'content': content,
            'size_bytes': size,
            'line_count': len(content),
            'type': chunk_type,
            'created_at': datetime.now().isoformat()
        }

class ParallelProcessor:
    """Handles parallel processing of log chunks"""
    
    def __init__(self, max_workers: int = None):
        self.max_workers = max_workers or min(cpu_count(), 8)  # Don't overwhelm system
        self.processing_stats = {}
        
    def process_chunks_parallel(self, chunks: List[Dict[str, Any]], 
                              processor_func: Callable) -> Dict[str, Any]:
        """
        Process chunks in parallel using multiprocessing
        Returns processing results and benchmarks
        """
        start_time = time.time()
        
        print(f"🚀 Starting parallel processing of {len(chunks)} chunks with {self.max_workers} workers...")
        
        results = []
        failed_chunks = []
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all chunks for processing
            future_to_chunk = {
                executor.submit(processor_func, chunk): chunk 
                for chunk in chunks
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_chunk):
                chunk = future_to_chunk[future]
                try:
                    result = future.result()
                    result['chunk_id'] = chunk['chunk_id']
                    results.append(result)
                    print(f"✅ Chunk {chunk['chunk_id']} processed successfully")
                except Exception as e:
                    failed_chunks.append({
                        'chunk_id': chunk['chunk_id'],
                        'error': str(e)
                    })
                    print(f"❌ Chunk {chunk['chunk_id']} failed: {e}")
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Calculate benchmarks
        benchmarks = {
            'total_chunks': len(chunks),
            'successful_chunks': len(results),
            'failed_chunks': len(failed_chunks),
            'processing_time_seconds': processing_time,
            'chunks_per_second': len(chunks) / processing_time if processing_time > 0 else 0,
            'parallel_workers': self.max_workers,
            'failed_chunk_details': failed_chunks
        }
        
        return {
            'results': results,
            'benchmarks': benchmarks,
            'success_rate': len(results) / len(chunks) if chunks else 0
        }
    
    def process_chunks_sequential(self, chunks: List[Dict[str, Any]], 
                                processor_func: Callable) -> Dict[str, Any]:
        """
        Process chunks sequentially for benchmarking comparison
        """
        start_time = time.time()
        
        print(f"🐌 Starting sequential processing of {len(chunks)} chunks...")
        
        results = []
        failed_chunks = []
        
        for chunk in chunks:
            try:
                result = processor_func(chunk)
                result['chunk_id'] = chunk['chunk_id']
                results.append(result)
                print(f"✅ Chunk {chunk['chunk_id']} processed")
            except Exception as e:
                failed_chunks.append({
                    'chunk_id': chunk['chunk_id'],
                    'error': str(e)
                })
                print(f"❌ Chunk {chunk['chunk_id']} failed: {e}")
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        benchmarks = {
            'total_chunks': len(chunks),
            'successful_chunks': len(results),
            'failed_chunks': len(failed_chunks),
            'processing_time_seconds': processing_time,
            'chunks_per_second': len(chunks) / processing_time if processing_time > 0 else 0,
            'parallel_workers': 1,
            'failed_chunk_details': failed_chunks
        }
        
        return {
            'results': results,
            'benchmarks': benchmarks,
            'success_rate': len(results) / len(chunks) if chunks else 0
        }
    
    def compare_processing_methods(self, chunks: List[Dict[str, Any]], 
                                 processor_func: Callable) -> Dict[str, Any]:
        """
        Compare parallel vs sequential processing performance
        """
        print("📊 Running processing benchmark comparison...")
        
        # Run sequential processing
        sequential_results = self.process_chunks_sequential(chunks.copy(), processor_func)
        
        # Run parallel processing
        parallel_results = self.process_chunks_parallel(chunks.copy(), processor_func)
        
        # Calculate speedup
        sequential_time = sequential_results['benchmarks']['processing_time_seconds']
        parallel_time = parallel_results['benchmarks']['processing_time_seconds']
        speedup = sequential_time / parallel_time if parallel_time > 0 else 0
        
        comparison = {
            'sequential': sequential_results['benchmarks'],
            'parallel': parallel_results['benchmarks'],
            'speedup_factor': speedup,
            'efficiency_percent': (speedup / self.max_workers) * 100 if self.max_workers > 0 else 0,
            'time_saved_seconds': sequential_time - parallel_time,
            'recommendation': self._get_processing_recommendation(speedup, self.max_workers)
        }
        
        return {
            'comparison': comparison,
            'parallel_results': parallel_results['results'],
            'sequential_results': sequential_results['results']
        }
    
    def _get_processing_recommendation(self, speedup: float, workers: int) -> str:
        """Generate recommendation based on benchmark results"""
        if speedup > workers * 0.7:  # Good parallel efficiency
            return f"Excellent parallel performance! {speedup:.1f}x speedup with {workers} workers."
        elif speedup > 2.0:
            return f"Good parallel performance. {speedup:.1f}x speedup achieved."
        elif speedup > 1.2:
            return f"Moderate parallel benefit. {speedup:.1f}x speedup. Consider optimizing worker tasks."
        else:
            return f"Limited parallel benefit. {speedup:.1f}x speedup. Sequential processing may be sufficient."

# Global instances
log_chunker = LogChunker()
parallel_processor = ParallelProcessor()
