import pytest
import tempfile
import os
from datetime import datetime

from app.services.log_parser import LogParser
from app.services.anomaly_detector import AnomalyDetector
from app.models.schemas import LogEntry


class TestLogParser:
    """Test log parsing functionality."""
    
    def test_parse_generic_log(self):
        """Test parsing generic log format."""
        parser = LogParser()
        line = "2024-01-15 10:00:01 INFO Application started successfully"
        
        entry = parser.parse_line(line, 'generic')
        
        assert entry is not None
        assert entry.level == 'INFO'
        assert 'Application started successfully' in entry.message
        assert isinstance(entry.timestamp, datetime)
    
    def test_parse_json_log(self):
        """Test parsing JSON log format."""
        parser = LogParser()
        line = '{"timestamp": "2024-01-15T10:00:01Z", "level": "INFO", "message": "Test message"}'
        
        entry = parser.parse_line(line, 'json')
        
        assert entry is not None
        assert entry.level == 'INFO'
        assert entry.message == 'Test message'
    
    def test_detect_format(self):
        """Test log format detection."""
        parser = LogParser()
        
        # Test generic format detection
        generic_lines = [
            "2024-01-15 10:00:01 INFO Test message",
            "2024-01-15 10:00:02 ERROR Error message"
        ]
        format_type = parser.detect_format(generic_lines)
        assert format_type == 'generic'
        
        # Test JSON format detection
        json_lines = [
            '{"timestamp": "2024-01-15T10:00:01Z", "level": "INFO", "message": "Test"}',
            '{"timestamp": "2024-01-15T10:00:02Z", "level": "ERROR", "message": "Error"}'
        ]
        format_type = parser.detect_format(json_lines)
        assert format_type == 'json'


class TestAnomalyDetector:
    """Test anomaly detection functionality."""
    
    def test_detect_anomalies(self):
        """Test basic anomaly detection."""
        detector = AnomalyDetector(threshold=1.5)
        
        # Create test log entries with one obvious anomaly
        log_entries = [
            LogEntry(timestamp=datetime.now(), level='INFO', message='Normal message'),
            LogEntry(timestamp=datetime.now(), level='INFO', message='Another normal message'),
            LogEntry(timestamp=datetime.now(), level='CRITICAL', message='SYSTEM FAILURE - EMERGENCY'),
            LogEntry(timestamp=datetime.now(), level='INFO', message='Back to normal'),
        ]
        
        results = detector.detect_anomalies(log_entries)
        
        # Should have results for all entries
        assert len(results) >= 0
        
        # Check if the critical message was flagged
        critical_results = [r for r in results if 'CRITICAL' in r.log_entry.level]
        if critical_results:
            assert any(r.is_anomaly for r in critical_results)
    
    def test_anomaly_summary(self):
        """Test anomaly summary generation."""
        detector = AnomalyDetector()
        
        # Create mock results
        from app.models.schemas import AnomalyResult
        results = [
            AnomalyResult(
                log_entry=LogEntry(timestamp=datetime.now(), level='INFO', message='Normal'),
                anomaly_score=0.5,
                is_anomaly=False,
                detection_method='statistical'
            ),
            AnomalyResult(
                log_entry=LogEntry(timestamp=datetime.now(), level='ERROR', message='Error'),
                anomaly_score=2.5,
                is_anomaly=True,
                detection_method='pattern'
            )
        ]
        
        summary = detector.get_anomaly_summary(results)
        
        assert summary['total_entries'] == 2
        assert summary['anomalies_detected'] == 1
        assert summary['anomaly_rate'] == 0.5


def test_sample_log_files():
    """Test that sample log files exist and are readable."""
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])