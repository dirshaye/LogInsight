"""
Log Merge and Deduplication Module - LogInsight Agent
Handles merging of cleaned log chunks and final deduplication
"""
import json
import sqlite3
import hashlib
from typing import List, Dict, Any, Optional
from datetime import datetime
from collections import defaultdict
import os

class LogMerger:
    """Handles merging and deduplication of processed log chunks"""
    
    def __init__(self, use_database: bool = True, db_path: Optional[str] = None):
        self.use_database = use_database
        self.db_path = db_path or "loginsight_temp.db"
        self.in_memory_store = []
        
        if self.use_database:
            self._init_database()
    
    def _init_database(self):
        """Initialize SQLite database for log storage"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create logs table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            log_hash TEXT UNIQUE NOT NULL,
            timestamp TEXT NOT NULL,
            level TEXT NOT NULL,
            message TEXT NOT NULL,
            service TEXT,
            user_id TEXT,
            request_id TEXT,
            source_ip TEXT,
            raw_data TEXT,
            created_at TEXT NOT NULL,
            chunk_id INTEGER
        )
        ''')
        
        # Create index for faster lookups
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_log_hash ON logs(log_hash)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON logs(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_level ON logs(level)')
        
        conn.commit()
        conn.close()
    
    def merge_chunks(self, processing_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Merge all processed chunks and deduplicate
        Returns merge statistics and final log count
        """
        start_time = datetime.now()
        
        print(f"🔄 Starting merge of {len(processing_results)} processed chunks...")
        
        merge_stats = {
            'total_chunks_processed': len(processing_results),
            'total_raw_logs': 0,
            'total_cleaned_logs': 0,
            'duplicates_removed': 0,
            'invalid_logs_filtered': 0,
            'final_log_count': 0,
            'merge_time_seconds': 0,
            'processing_errors': []
        }
        
        # Track duplicates across chunks
        seen_hashes = set()
        chunk_stats = []
        
        for result in processing_results:
            if not result['success']:
                merge_stats['processing_errors'].append({
                    'chunk_id': result['chunk_id'],
                    'error': result['error']
                })
                continue
            
            chunk_stat = {
                'chunk_id': result['chunk_id'],
                'original_count': result['original_line_count'],
                'cleaned_count': result['cleaned_line_count'],
                'tokens_used': result['tokens_used'],
                'processing_time': result['processing_time_seconds']
            }
            
            merge_stats['total_raw_logs'] += result['original_line_count']
            merge_stats['total_cleaned_logs'] += result['cleaned_line_count']
            
            # Process each cleaned log
            for log_entry in result['cleaned_logs']:
                # Generate hash for deduplication
                log_hash = self._generate_log_hash(log_entry)
                
                if log_hash not in seen_hashes:
                    seen_hashes.add(log_hash)
                    
                    # Store the log
                    if self._store_log(log_entry, log_hash, result['chunk_id']):
                        merge_stats['final_log_count'] += 1
                    else:
                        merge_stats['invalid_logs_filtered'] += 1
                else:
                    merge_stats['duplicates_removed'] += 1
            
            chunk_stats.append(chunk_stat)
        
        end_time = datetime.now()
        merge_stats['merge_time_seconds'] = (end_time - start_time).total_seconds()
        merge_stats['chunk_details'] = chunk_stats
        
        print(f"✅ Merge completed: {merge_stats['final_log_count']} final logs")
        print(f"   Removed {merge_stats['duplicates_removed']} duplicates")
        print(f"   Filtered {merge_stats['invalid_logs_filtered']} invalid logs")
        
        return merge_stats
    
    def _generate_log_hash(self, log_entry: Dict[str, Any]) -> str:
        """Generate unique hash for log entry based on key fields"""
        # Use timestamp, level, message, and service for uniqueness
        hash_components = [
            log_entry.get('timestamp', ''),
            log_entry.get('level', ''),
            log_entry.get('message', ''),
            log_entry.get('service', '')
        ]
        
        hash_string = '|'.join(str(comp) for comp in hash_components)
        return hashlib.md5(hash_string.encode('utf-8')).hexdigest()
    
    def _store_log(self, log_entry: Dict[str, Any], log_hash: str, chunk_id: int) -> bool:
        """Store log entry in database or memory"""
        try:
            if self.use_database:
                return self._store_log_in_db(log_entry, log_hash, chunk_id)
            else:
                return self._store_log_in_memory(log_entry, log_hash, chunk_id)
        except Exception as e:
            print(f"❌ Error storing log: {e}")
            return False
    
    def _store_log_in_db(self, log_entry: Dict[str, Any], log_hash: str, chunk_id: int) -> bool:
        """Store log in SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
            INSERT INTO logs (
                log_hash, timestamp, level, message, service,
                user_id, request_id, source_ip, raw_data, created_at, chunk_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                log_hash,
                log_entry.get('timestamp'),
                log_entry.get('level'),
                log_entry.get('message'),
                log_entry.get('service'),
                log_entry.get('user_id'),
                log_entry.get('request_id'),
                log_entry.get('source_ip'),
                json.dumps(log_entry),
                datetime.now().isoformat(),
                chunk_id
            ))
            
            conn.commit()
            return True
            
        except sqlite3.IntegrityError:
            # Duplicate hash - already exists
            return False
        finally:
            conn.close()
    
    def _store_log_in_memory(self, log_entry: Dict[str, Any], log_hash: str, chunk_id: int) -> bool:
        """Store log in memory"""
        enhanced_log = log_entry.copy()
        enhanced_log['_log_hash'] = log_hash
        enhanced_log['_chunk_id'] = chunk_id
        enhanced_log['_stored_at'] = datetime.now().isoformat()
        
        self.in_memory_store.append(enhanced_log)
        return True
    
    def get_cleaned_logs(self, limit: Optional[int] = None, 
                        level_filter: Optional[str] = None,
                        service_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Retrieve cleaned and merged logs with optional filtering"""
        
        if self.use_database:
            return self._get_logs_from_db(limit, level_filter, service_filter)
        else:
            return self._get_logs_from_memory(limit, level_filter, service_filter)
    
    def _get_logs_from_db(self, limit: Optional[int] = None,
                         level_filter: Optional[str] = None,
                         service_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get logs from database with filtering"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Build query with filters
        query = "SELECT raw_data FROM logs WHERE 1=1"
        params = []
        
        if level_filter:
            query += " AND level = ?"
            params.append(level_filter.upper())
        
        if service_filter:
            query += " AND service = ?"
            params.append(service_filter)
        
        query += " ORDER BY timestamp DESC"
        
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        # Parse JSON data
        logs = []
        for (raw_data,) in rows:
            try:
                log_entry = json.loads(raw_data)
                logs.append(log_entry)
            except json.JSONDecodeError:
                continue
        
        return logs
    
    def _get_logs_from_memory(self, limit: Optional[int] = None,
                             level_filter: Optional[str] = None,
                             service_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get logs from memory with filtering"""
        filtered_logs = self.in_memory_store.copy()
        
        # Apply filters
        if level_filter:
            filtered_logs = [log for log in filtered_logs 
                           if log.get('level', '').upper() == level_filter.upper()]
        
        if service_filter:
            filtered_logs = [log for log in filtered_logs 
                           if log.get('service') == service_filter]
        
        # Sort by timestamp (newest first)
        try:
            filtered_logs.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        except:
            pass  # If timestamp parsing fails, keep original order
        
        # Apply limit
        if limit:
            filtered_logs = filtered_logs[:limit]
        
        return filtered_logs
    
    def get_log_statistics(self) -> Dict[str, Any]:
        """Get statistics about stored logs"""
        if self.use_database:
            return self._get_db_statistics()
        else:
            return self._get_memory_statistics()
    
    def _get_db_statistics(self) -> Dict[str, Any]:
        """Get statistics from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total count
        cursor.execute("SELECT COUNT(*) FROM logs")
        total_count = cursor.fetchone()[0]
        
        # Count by level
        cursor.execute("SELECT level, COUNT(*) FROM logs GROUP BY level")
        level_counts = dict(cursor.fetchall())
        
        # Count by service
        cursor.execute("SELECT service, COUNT(*) FROM logs GROUP BY service LIMIT 10")
        service_counts = dict(cursor.fetchall())
        
        # Time range
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM logs")
        time_range = cursor.fetchone()
        
        conn.close()
        
        return {
            'total_logs': total_count,
            'level_distribution': level_counts,
            'top_services': service_counts,
            'time_range': {
                'earliest': time_range[0],
                'latest': time_range[1]
            },
            'storage_type': 'database'
        }
    
    def _get_memory_statistics(self) -> Dict[str, Any]:
        """Get statistics from memory store"""
        if not self.in_memory_store:
            return {'total_logs': 0, 'storage_type': 'memory'}
        
        # Count by level
        level_counts = defaultdict(int)
        service_counts = defaultdict(int)
        
        timestamps = []
        
        for log in self.in_memory_store:
            level_counts[log.get('level', 'UNKNOWN')] += 1
            service_counts[log.get('service', 'unknown')] += 1
            if log.get('timestamp'):
                timestamps.append(log['timestamp'])
        
        # Get top services
        top_services = dict(sorted(service_counts.items(), 
                                 key=lambda x: x[1], reverse=True)[:10])
        
        return {
            'total_logs': len(self.in_memory_store),
            'level_distribution': dict(level_counts),
            'top_services': top_services,
            'time_range': {
                'earliest': min(timestamps) if timestamps else None,
                'latest': max(timestamps) if timestamps else None
            },
            'storage_type': 'memory'
        }
    
    def cleanup(self):
        """Clean up resources"""
        if self.use_database and os.path.exists(self.db_path):
            try:
                os.remove(self.db_path)
                print(f"🗑️ Cleaned up database file: {self.db_path}")
            except:
                pass
        
        self.in_memory_store.clear()

# Global merger instance
log_merger = LogMerger()
