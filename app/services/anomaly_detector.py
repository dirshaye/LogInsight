import numpy as np
import pandas as pd
from typing import List, Tuple, Dict, Any
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta
import re

from app.models.schemas import LogEntry, AnomalyResult


class AnomalyDetector:
    """Anomaly detection for log entries using multiple methods."""
    
    def __init__(self, threshold: float = 2.0):
        self.threshold = threshold
        self.tfidf_vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words='english',
            ngram_range=(1, 2)
        )
        self.isolation_forest = IsolationForest(
            contamination=0.1,
            random_state=42
        )
        self.scaler = StandardScaler()
        self.is_fitted = False
    
    def detect_anomalies(self, log_entries: List[LogEntry]) -> List[AnomalyResult]:
        """Detect anomalies in log entries using multiple methods."""
        if not log_entries:
            return []
        
        results = []
        
        # Convert to DataFrame for easier processing
        df = self._logs_to_dataframe(log_entries)
        
        # Statistical anomaly detection
        statistical_anomalies = self._detect_statistical_anomalies(df)
        
        # Pattern-based anomaly detection
        pattern_anomalies = self._detect_pattern_anomalies(df)
        
        # Time-series anomaly detection
        temporal_anomalies = self._detect_temporal_anomalies(df)
        
        # Message content anomaly detection
        content_anomalies = self._detect_content_anomalies(df)
        
        # Combine results
        for i, log_entry in enumerate(log_entries):
            anomaly_score = 0.0
            methods = []
            explanations = []
            
            if i in statistical_anomalies:
                anomaly_score += statistical_anomalies[i]
                methods.append("statistical")
                explanations.append("Statistical deviation detected")
            
            if i in pattern_anomalies:
                anomaly_score += pattern_anomalies[i]
                methods.append("pattern")
                explanations.append("Unusual pattern detected")
            
            if i in temporal_anomalies:
                anomaly_score += temporal_anomalies[i]
                methods.append("temporal")
                explanations.append("Temporal anomaly detected")
            
            if i in content_anomalies:
                anomaly_score += content_anomalies[i]
                methods.append("content")
                explanations.append("Content anomaly detected")
            
            is_anomaly = anomaly_score > self.threshold
            
            if is_anomaly or anomaly_score > 0:
                results.append(AnomalyResult(
                    log_entry=log_entry,
                    anomaly_score=anomaly_score,
                    is_anomaly=is_anomaly,
                    detection_method=", ".join(methods) if methods else "none",
                    explanation="; ".join(explanations) if explanations else None
                ))
        
        return results
    
    def _logs_to_dataframe(self, log_entries: List[LogEntry]) -> pd.DataFrame:
        """Convert log entries to pandas DataFrame."""
        data = []
        for entry in log_entries:
            data.append({
                'timestamp': entry.timestamp,
                'level': entry.level,
                'message': entry.message,
                'source': entry.source or 'unknown',
                'message_length': len(entry.message),
                'word_count': len(entry.message.split()),
                'has_numbers': bool(re.search(r'\d', entry.message)),
                'has_special_chars': bool(re.search(r'[!@#$%^&*(),.?":{}|<>]', entry.message)),
                'error_keywords': self._count_error_keywords(entry.message),
            })
        
        return pd.DataFrame(data)
    
    def _count_error_keywords(self, message: str) -> int:
        """Count error-related keywords in message."""
        error_keywords = [
            'error', 'exception', 'fail', 'fault', 'crash', 'abort',
            'timeout', 'refused', 'denied', 'invalid', 'corrupt',
            'unable', 'cannot', 'forbidden', 'unauthorized'
        ]
        message_lower = message.lower()
        return sum(1 for keyword in error_keywords if keyword in message_lower)
    
    def _detect_statistical_anomalies(self, df: pd.DataFrame) -> Dict[int, float]:
        """Detect statistical anomalies in numerical features."""
        anomalies = {}
        
        numerical_cols = ['message_length', 'word_count', 'error_keywords']
        
        for col in numerical_cols:
            if col in df.columns:
                values = df[col].values
                mean = np.mean(values)
                std = np.std(values)
                
                if std > 0:
                    z_scores = np.abs((values - mean) / std)
                    
                    for i, z_score in enumerate(z_scores):
                        if z_score > self.threshold:
                            if i not in anomalies:
                                anomalies[i] = 0
                            anomalies[i] += z_score / len(numerical_cols)
        
        return anomalies
    
    def _detect_pattern_anomalies(self, df: pd.DataFrame) -> Dict[int, float]:
        """Detect pattern-based anomalies."""
        anomalies = {}
        
        # Log level anomalies
        level_counts = df['level'].value_counts()
        rare_levels = level_counts[level_counts < len(df) * 0.05].index
        
        for i, level in enumerate(df['level']):
            if level in rare_levels:
                anomalies[i] = 1.0
        
        # Source anomalies
        if 'source' in df.columns:
            source_counts = df['source'].value_counts()
            rare_sources = source_counts[source_counts < len(df) * 0.02].index
            
            for i, source in enumerate(df['source']):
                if source in rare_sources:
                    if i not in anomalies:
                        anomalies[i] = 0
                    anomalies[i] += 0.5
        
        return anomalies
    
    def _detect_temporal_anomalies(self, df: pd.DataFrame) -> Dict[int, float]:
        """Detect temporal anomalies."""
        anomalies = {}
        
        if len(df) < 2:
            return anomalies
        
        # Sort by timestamp
        df_sorted = df.sort_values('timestamp').reset_index(drop=True)
        
        # Calculate time differences
        time_diffs = []
        for i in range(1, len(df_sorted)):
            diff = (df_sorted.iloc[i]['timestamp'] - df_sorted.iloc[i-1]['timestamp']).total_seconds()
            time_diffs.append(diff)
        
        if time_diffs:
            mean_diff = np.mean(time_diffs)
            std_diff = np.std(time_diffs)
            
            if std_diff > 0:
                for i, diff in enumerate(time_diffs):
                    z_score = abs((diff - mean_diff) / std_diff)
                    if z_score > self.threshold:
                        anomalies[i + 1] = z_score / 2
        
        return anomalies
    
    def _detect_content_anomalies(self, df: pd.DataFrame) -> Dict[int, float]:
        """Detect content-based anomalies using TF-IDF and clustering."""
        anomalies = {}
        
        if len(df) < 10:  # Need sufficient data for content analysis
            return anomalies
        
        try:
            # Vectorize messages
            messages = df['message'].fillna('').astype(str)
            tfidf_matrix = self.tfidf_vectorizer.fit_transform(messages)
            
            # Use Isolation Forest for anomaly detection
            self.isolation_forest.fit(tfidf_matrix.toarray())
            anomaly_scores = self.isolation_forest.decision_function(tfidf_matrix.toarray())
            predictions = self.isolation_forest.predict(tfidf_matrix.toarray())
            
            # Normalize scores to positive values
            min_score = np.min(anomaly_scores)
            max_score = np.max(anomaly_scores)
            
            for i, (score, prediction) in enumerate(zip(anomaly_scores, predictions)):
                if prediction == -1:  # Anomaly
                    # Normalize score between 0 and 1
                    normalized_score = (max_score - score) / (max_score - min_score) if max_score != min_score else 0.5
                    anomalies[i] = normalized_score * 2  # Scale up
        
        except Exception as e:
            # Fallback to simple keyword-based detection
            error_keywords = ['error', 'exception', 'fail', 'crash']
            for i, message in enumerate(df['message']):
                error_count = sum(1 for keyword in error_keywords if keyword.lower() in message.lower())
                if error_count > 0:
                    anomalies[i] = min(error_count * 0.5, 2.0)
        
        return anomalies
    
    def get_anomaly_summary(self, results: List[AnomalyResult]) -> Dict[str, Any]:
        """Get summary statistics of anomaly detection results."""
        if not results:
            return {
                'total_entries': 0,
                'anomalies_detected': 0,
                'anomaly_rate': 0.0,
                'avg_anomaly_score': 0.0,
                'methods_used': [],
                'level_distribution': {}
            }
        
        anomalies = [r for r in results if r.is_anomaly]
        methods_used = set()
        level_distribution = {}
        
        for result in results:
            if result.detection_method:
                methods_used.update(result.detection_method.split(', '))
            
            level = result.log_entry.level
            level_distribution[level] = level_distribution.get(level, 0) + 1
        
        return {
            'total_entries': len(results),
            'anomalies_detected': len(anomalies),
            'anomaly_rate': len(anomalies) / len(results) if results else 0.0,
            'avg_anomaly_score': np.mean([r.anomaly_score for r in results]),
            'methods_used': list(methods_used),
            'level_distribution': level_distribution
        }