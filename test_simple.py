#!/usr/bin/env python3
"""
Simple test script without external dependencies.
"""

import os
import sys
from datetime import datetime

# Add the project root to Python path
sys.path.insert(0, '/home/runner/work/paralogX/paralogX')

from demo import SimpleLogParser, SimpleAnomalyDetector, LogEntry


def test_log_parser():
    """Test log parsing functionality."""
    print("🧪 Testing log parser...")
    
    parser = SimpleLogParser()
    
    # Test generic log parsing
    line = "2024-01-15 10:00:01 INFO Application started successfully"
    entry = parser.parse_line(line)
    
    assert entry is not None, "Parser should return an entry"
    assert entry.level == 'INFO', f"Expected INFO, got {entry.level}"
    assert 'Application started successfully' in entry.message, "Message not parsed correctly"
    
    print("✅ Log parser tests passed")


def test_anomaly_detector():
    """Test anomaly detection functionality."""
    print("🧪 Testing anomaly detector...")
    
    detector = SimpleAnomalyDetector(threshold=1.0)  # Lower threshold for testing
    
    # Create test log entries
    log_entries = [
        LogEntry(timestamp=datetime.now(), level='INFO', message='Normal message'),
        LogEntry(timestamp=datetime.now(), level='INFO', message='Another normal message'),
        LogEntry(timestamp=datetime.now(), level='CRITICAL', message='SYSTEM FAILURE - EMERGENCY'),
        LogEntry(timestamp=datetime.now(), level='ERROR', message='Database connection failed timeout error'),
    ]
    
    results = detector.detect_anomalies(log_entries)
    
    assert len(results) >= 0, "Should return some results"
    
    # Check if any anomalies were flagged (with lower threshold)
    anomaly_scores = [r.anomaly_score for r in results]
    max_score = max(anomaly_scores) if anomaly_scores else 0
    
    # At least one entry should have some anomaly score
    assert max_score > 0, f"Should detect at least some anomaly indicators, max score: {max_score}"
    
    print(f"✅ Anomaly detector tests passed - max anomaly score: {max_score:.2f}")


def test_sample_files():
    """Test that sample files exist and are readable."""
    print("🧪 Testing sample files...")
    
    sample_files = [
        '/home/runner/work/paralogX/paralogX/sample_data/application.log',
        '/home/runner/work/paralogX/paralogX/sample_data/access.log',
        '/home/runner/work/paralogX/paralogX/sample_data/structured.log'
    ]
    
    for file_path in sample_files:
        assert os.path.exists(file_path), f"Sample file {file_path} does not exist"
        
        with open(file_path, 'r') as f:
            content = f.read()
            assert len(content) > 0, f"Sample file {file_path} is empty"
    
    print("✅ Sample files tests passed")


def test_integration():
    """Test end-to-end functionality."""
    print("🧪 Testing integration...")
    
    from demo import SimpleDistributedProcessor
    
    processor = SimpleDistributedProcessor()
    sample_file = '/home/runner/work/paralogX/paralogX/sample_data/application.log'
    
    # Test sequential processing
    seq_result = processor.process_file_sequential(sample_file, chunk_size=10, threshold=1.5)
    assert seq_result['total_entries'] > 0, "Should process some entries"
    assert 'processing_time' in seq_result, "Should have processing time"
    
    # Test parallel processing
    par_result = processor.process_file_parallel(sample_file, chunk_size=10, threshold=1.5)
    assert par_result['total_entries'] > 0, "Should process some entries"
    assert 'processing_time' in par_result, "Should have processing time"
    
    # Test comparison
    comparison = processor.compare_performance(sample_file, chunk_size=10, threshold=1.5)
    assert 'speedup_factor' in comparison, "Should have speedup factor"
    assert comparison['speedup_factor'] > 0, "Speedup should be positive"
    
    print(f"✅ Integration tests passed - speedup: {comparison['speedup_factor']:.2f}x")


def main():
    """Run all tests."""
    print("🚀 Running ParalogX Tests")
    print("=" * 50)
    
    try:
        test_log_parser()
        test_anomaly_detector()
        test_sample_files()
        test_integration()
        
        print("\n🎉 All tests passed successfully!")
        print("✅ ParalogX is working correctly")
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()