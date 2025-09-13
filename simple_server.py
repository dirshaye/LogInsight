#!/usr/bin/env python3
"""
Simplified FastAPI version of ParalogX using only built-in Python libraries.
This demonstrates the web API functionality without external dependencies.
"""

import json
import os
import time
import tempfile
from datetime import datetime
from typing import Dict, Any, List, Optional
from urllib.parse import parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

# Import our demo functionality
from demo import SimpleDistributedProcessor, create_large_sample_log


class ParalogXHandler(BaseHTTPRequestHandler):
    """HTTP request handler for ParalogX API."""
    
    def __init__(self, *args, **kwargs):
        self.processor = SimpleDistributedProcessor()
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests."""
        if self.path == '/':
            self.serve_dashboard()
        elif self.path == '/health':
            self.serve_health()
        elif self.path == '/api/demo':
            self.serve_demo()
        elif self.path == '/api/metrics':
            self.serve_metrics()
        else:
            self.send_error(404, "Not Found")
    
    def do_POST(self):
        """Handle POST requests."""
        if self.path == '/api/analyze':
            self.handle_analyze()
        elif self.path == '/api/compare':
            self.handle_compare()
        else:
            self.send_error(404, "Not Found")
    
    def serve_dashboard(self):
        """Serve the main dashboard."""
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>ParalogX - Parallel Log Analyzer</title>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
                .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                h1 { color: #333; text-align: center; margin-bottom: 30px; }
                .feature-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 30px; }
                .feature-card { background: #f8f9fa; padding: 20px; border-radius: 8px; border-left: 4px solid #007bff; }
                .feature-card h3 { color: #007bff; margin-top: 0; }
                .api-section { background: #e9ecef; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
                .endpoint { background: #fff; padding: 15px; margin: 10px 0; border-radius: 5px; border-left: 3px solid #28a745; }
                .button { display: inline-block; padding: 10px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px; margin: 5px; }
                .button:hover { background: #0056b3; }
                .demo-results { background: #d4edda; padding: 15px; border-radius: 5px; margin: 10px 0; }
                .performance { font-family: monospace; background: #f8f9fa; padding: 10px; border-radius: 3px; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🚀 ParalogX - Parallel Log Analyzer</h1>
                
                <div class="feature-grid">
                    <div class="feature-card">
                        <h3>⚡ Parallel Processing</h3>
                        <p>Distributed processing using threading with automatic speedup detection and performance metrics.</p>
                    </div>
                    <div class="feature-card">
                        <h3>🔍 Anomaly Detection</h3>
                        <p>Multi-method detection: statistical analysis, pattern recognition, keyword matching, and security analysis.</p>
                    </div>
                    <div class="feature-card">
                        <h3>📊 Performance Monitoring</h3>
                        <p>Real-time comparison of sequential vs parallel processing with detailed performance metrics.</p>
                    </div>
                    <div class="feature-card">
                        <h3>🔧 Format Support</h3>
                        <p>Auto-detection and parsing of Apache, Nginx, syslog, JSON, and generic log formats.</p>
                    </div>
                </div>

                <div class="api-section">
                    <h3>🔗 API Endpoints</h3>
                    <div class="endpoint">
                        <strong>GET /health</strong> - Health check and system status
                    </div>
                    <div class="endpoint">
                        <strong>GET /api/demo</strong> - Run demonstration analysis
                    </div>
                    <div class="endpoint">
                        <strong>POST /api/analyze</strong> - Analyze log file for anomalies
                    </div>
                    <div class="endpoint">
                        <strong>POST /api/compare</strong> - Compare parallel vs sequential performance
                    </div>
                    <div class="endpoint">
                        <strong>GET /api/metrics</strong> - Get system performance metrics
                    </div>
                </div>

                <div style="text-align: center; margin: 30px 0;">
                    <button class="button" onclick="runDemo()">🚀 Run Demo Analysis</button>
                    <button class="button" onclick="checkHealth()">🔍 Health Check</button>
                    <button class="button" onclick="getMetrics()">📊 Get Metrics</button>
                </div>

                <div id="results" style="margin-top: 30px;"></div>
            </div>

            <script>
                async function runDemo() {
                    const resultsDiv = document.getElementById('results');
                    resultsDiv.innerHTML = '<p>🔄 Running demo analysis...</p>';
                    
                    try {
                        const response = await fetch('/api/demo');
                        const data = await response.json();
                        
                        let html = '<div class="demo-results">';
                        html += '<h3>📈 Demo Analysis Results</h3>';
                        html += `<div class="performance">`;
                        html += `Sequential: ${data.sequential.processing_time.toFixed(4)}s<br>`;
                        html += `Parallel: ${data.parallel.processing_time.toFixed(4)}s<br>`;
                        html += `Speedup: ${data.speedup_factor.toFixed(2)}x<br>`;
                        html += `Entries: ${data.parallel.total_entries}<br>`;
                        html += `Anomalies: ${data.parallel.anomalies_detected}`;
                        html += '</div></div>';
                        
                        resultsDiv.innerHTML = html;
                    } catch (error) {
                        resultsDiv.innerHTML = '<p style="color: red;">❌ Error running demo: ' + error.message + '</p>';
                    }
                }

                async function checkHealth() {
                    const resultsDiv = document.getElementById('results');
                    resultsDiv.innerHTML = '<p>🔄 Checking health...</p>';
                    
                    try {
                        const response = await fetch('/health');
                        const data = await response.json();
                        
                        let html = '<div class="demo-results">';
                        html += '<h3>🏥 Health Status</h3>';
                        html += `<div class="performance">Status: ${data.status}<br>`;
                        html += `Timestamp: ${data.timestamp}<br>`;
                        html += `Version: ${data.version}</div></div>`;
                        
                        resultsDiv.innerHTML = html;
                    } catch (error) {
                        resultsDiv.innerHTML = '<p style="color: red;">❌ Health check failed: ' + error.message + '</p>';
                    }
                }

                async function getMetrics() {
                    const resultsDiv = document.getElementById('results');
                    resultsDiv.innerHTML = '<p>🔄 Getting metrics...</p>';
                    
                    try {
                        const response = await fetch('/api/metrics');
                        const data = await response.json();
                        
                        let html = '<div class="demo-results">';
                        html += '<h3>📊 System Metrics</h3>';
                        html += `<div class="performance">`;
                        html += `Processor: ${data.processor_type}<br>`;
                        html += `Supported Methods: ${data.detection_methods.join(', ')}<br>`;
                        html += `Log Formats: ${data.log_formats.join(', ')}`;
                        html += '</div></div>';
                        
                        resultsDiv.innerHTML = html;
                    } catch (error) {
                        resultsDiv.innerHTML = '<p style="color: red;">❌ Error getting metrics: ' + error.message + '</p>';
                    }
                }
            </script>
        </body>
        </html>
        """
        
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(html.encode())
    
    def serve_health(self):
        """Serve health check."""
        response = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'version': '1.0.0',
            'processor': 'ThreadPoolExecutor'
        }
        
        self.send_json_response(response)
    
    def serve_demo(self):
        """Run demo analysis."""
        try:
            # Create or use existing sample file
            sample_dir = '/home/runner/work/paralogX/paralogX/sample_data'
            sample_file = os.path.join(sample_dir, 'application.log')
            
            if not os.path.exists(sample_file):
                # Create a quick demo file
                demo_file = os.path.join(tempfile.gettempdir(), 'demo.log')
                with open(demo_file, 'w') as f:
                    f.write("2024-01-15 10:00:01 INFO Application started\n")
                    f.write("2024-01-15 10:00:02 INFO User login successful\n")
                    f.write("2024-01-15 10:00:03 ERROR Database connection failed\n")
                    f.write("2024-01-15 10:00:04 CRITICAL System failure detected\n")
                sample_file = demo_file
            
            result = self.processor.compare_performance(sample_file, chunk_size=10, threshold=1.5)
            self.send_json_response(result)
            
        except Exception as e:
            self.send_json_response({'error': str(e)}, status=500)
    
    def serve_metrics(self):
        """Serve system metrics."""
        response = {
            'processor_type': 'ThreadPoolExecutor',
            'detection_methods': ['statistical', 'pattern', 'keyword', 'security'],
            'log_formats': ['generic', 'json', 'apache', 'nginx', 'syslog'],
            'parallel_workers': 4,
            'chunk_size_default': 100,
            'anomaly_threshold_default': 2.0
        }
        
        self.send_json_response(response)
    
    def handle_analyze(self):
        """Handle log analysis request."""
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length:
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode('utf-8'))
                
                file_path = data.get('file_path')
                parallel = data.get('parallel', True)
                chunk_size = data.get('chunk_size', 100)
                threshold = data.get('threshold', 2.0)
                
                if not file_path or not os.path.exists(file_path):
                    self.send_json_response({'error': 'File not found'}, status=404)
                    return
                
                if parallel:
                    result = self.processor.process_file_parallel(file_path, chunk_size, threshold)
                else:
                    result = self.processor.process_file_sequential(file_path, chunk_size, threshold)
                
                self.send_json_response(result)
            else:
                self.send_json_response({'error': 'No data provided'}, status=400)
                
        except Exception as e:
            self.send_json_response({'error': str(e)}, status=500)
    
    def handle_compare(self):
        """Handle performance comparison request."""
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length:
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode('utf-8'))
                
                file_path = data.get('file_path')
                chunk_size = data.get('chunk_size', 100)
                threshold = data.get('threshold', 2.0)
                
                if not file_path or not os.path.exists(file_path):
                    self.send_json_response({'error': 'File not found'}, status=404)
                    return
                
                result = self.processor.compare_performance(file_path, chunk_size, threshold)
                self.send_json_response(result)
            else:
                self.send_json_response({'error': 'No data provided'}, status=400)
                
        except Exception as e:
            self.send_json_response({'error': str(e)}, status=500)
    
    def send_json_response(self, data: Dict[str, Any], status: int = 200):
        """Send JSON response."""
        self.send_response(status)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode())
    
    def log_message(self, format, *args):
        """Override to reduce logging noise."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {format % args}")


def run_server(port: int = 8000):
    """Run the ParalogX HTTP server."""
    server_address = ('', port)
    httpd = HTTPServer(server_address, ParalogXHandler)
    print(f"🚀 ParalogX server starting on http://localhost:{port}")
    print(f"📊 Dashboard: http://localhost:{port}")
    print(f"🔍 Health check: http://localhost:{port}/health")
    print(f"🎮 Demo API: http://localhost:{port}/api/demo")
    print("\nPress Ctrl+C to stop the server...")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
        httpd.shutdown()


if __name__ == "__main__":
    run_server()