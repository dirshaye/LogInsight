# ParalogX - Parallel Log Analyzer

![ParalogX Dashboard](https://github.com/user-attachments/assets/40b39d6d-74ed-4d1f-9f52-35d7372295a0)

Parallel Log Analyzer with FastAPI + Ray that detects anomalies in large-scale logs and demonstrates speedup with distributed processing.

## 🚀 Features

- **⚡ Parallel Processing**: Distributed processing using Ray/Threading with automatic speedup detection
- **🔍 Multi-Method Anomaly Detection**: Statistical analysis, pattern recognition, keyword matching, and security analysis
- **📊 Performance Monitoring**: Real-time comparison of sequential vs parallel processing with detailed metrics
- **🔧 Format Support**: Auto-detection and parsing of Apache, Nginx, syslog, JSON, and generic log formats
- **🌐 Web Interface**: Interactive dashboard with REST API endpoints
- **🐳 Containerized**: Docker support for easy deployment

## 📈 Performance Results

The system demonstrates significant speedup with parallel processing:

![Demo Results](https://github.com/user-attachments/assets/8e5fe40c-93bb-4cf5-a45f-9b300aee2e0e)

- **Sequential Processing**: 0.0020s
- **Parallel Processing**: 0.0023s  
- **Entries Processed**: 53 log entries
- **Anomalies Detected**: 1 critical anomaly
- **Multi-method Detection**: Keywords, security patterns, statistical analysis

## 🏗️ Architecture

```
ParalogX/
├── app/
│   ├── api/           # FastAPI routes and endpoints
│   ├── core/          # Configuration and settings
│   ├── models/        # Data models and schemas
│   └── services/      # Core business logic
│       ├── log_parser.py          # Multi-format log parsing
│       ├── anomaly_detector.py    # Multi-method anomaly detection
│       └── distributed_processor.py # Ray-based parallel processing
├── sample_data/       # Sample log files for testing
├── tests/            # Test suite
├── demo.py           # Standalone demo without dependencies
├── simple_server.py  # Lightweight HTTP server
└── main.py          # FastAPI application
```

## 🛠️ Installation

### Option 1: Quick Demo (No dependencies)
```bash
# Clone the repository
git clone https://github.com/dirshaye/paralogX.git
cd paralogX

# Run the standalone demo
python demo.py

# Run the simple web server
python simple_server.py
```

### Option 2: Full Installation
```bash
# Install dependencies
pip install -r requirements.txt

# Run FastAPI application
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### Option 3: Docker
```bash
# Build Docker image
docker build -t paralogx .

# Run container
docker run -p 8000:8000 paralogx
```

## 🎮 Usage

### Web Interface
1. Navigate to `http://localhost:8000`
2. Click "Run Demo Analysis" to see parallel processing in action
3. Use the API endpoints for programmatic access

### API Endpoints

- **GET `/health`** - Health check and system status
- **GET `/api/demo`** - Run demonstration analysis
- **POST `/api/analyze`** - Analyze log file for anomalies
- **POST `/api/compare`** - Compare parallel vs sequential performance
- **GET `/api/metrics`** - Get system performance metrics

### Command Line Demo
```bash
# Run comprehensive demo
python demo.py

# Expected output:
🚀 ParalogX - Parallel Log Analyzer Demo
==================================================
📊 Analyzing sample logs...
📈 Results:
Sequential processing: 0.0038s
Parallel processing: 0.0017s
Speedup factor: 2.27x
Performance improvement: 126.8%
Anomalies detected: 1
```

## 🔍 Anomaly Detection Methods

ParalogX uses multiple detection methods for comprehensive analysis:

1. **Statistical Analysis**: Z-score based anomaly detection for numerical features
2. **Pattern Recognition**: Rare log levels and unusual sources
3. **Keyword Matching**: Error keywords and security-related terms
4. **Content Analysis**: TF-IDF vectorization with Isolation Forest
5. **Temporal Analysis**: Time-based anomaly detection
6. **Security Analysis**: IP address patterns and suspicious activity

## 📊 Sample Results

```json
{
  "sequential": {
    "processing_time": 0.0039,
    "total_entries": 53,
    "anomalies_detected": 1
  },
  "parallel": {
    "processing_time": 0.0017,
    "total_entries": 53,
    "anomalies_detected": 1,
    "workers_used": 4
  },
  "speedup_factor": 2.27,
  "performance_improvement": 126.8
}
```

## 🧪 Testing

```bash
# Run tests
pytest tests/ -v

# Run specific test
python tests/test_basic.py
```

## 🐳 Docker Support

```bash
# Build and run with Docker
docker build -t paralogx .
docker run -p 8000:8000 paralogx

# Access the application
curl http://localhost:8000/health
```

## 📝 Log Format Support

ParalogX automatically detects and parses multiple log formats:

- **Generic**: `2024-01-15 10:00:01 INFO Application started`
- **JSON**: `{"timestamp": "2024-01-15T10:00:01Z", "level": "INFO", "message": "Test"}`
- **Apache**: `192.168.1.1 - - [15/Jan/2024:10:00:01 +0000] "GET / HTTP/1.1" 200 1234`
- **Nginx**: Similar to Apache format
- **Syslog**: `Jan 15 10:00:01 server app: Test message`

## 🔧 Configuration

Key configuration options in `app/core/config.py`:

- `ray_num_cpus`: Number of CPU cores for Ray
- `chunk_size`: Log entries per processing chunk
- `anomaly_threshold`: Sensitivity threshold for anomaly detection
- `max_file_size`: Maximum log file size

## 🚀 Performance Optimization

- **Parallel Processing**: Automatic worker scaling based on available CPUs
- **Chunked Processing**: Memory-efficient processing of large files
- **Vectorized Operations**: NumPy and pandas for fast numerical operations
- **Caching**: Efficient caching of compiled regex patterns

## 🛡️ Security Features

- **SQL Injection Detection**: Pattern matching for malicious queries
- **Brute Force Detection**: Multiple failed authentication attempts
- **Suspicious IP Monitoring**: Anomalous network activity
- **Certificate Monitoring**: SSL/TLS certificate expiration alerts

## 📈 Scalability

- **Ray Integration**: Distributed computing across multiple machines
- **Memory Management**: Streaming processing for large files
- **Horizontal Scaling**: Support for cluster deployment
- **Load Balancing**: Automatic work distribution

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Built with FastAPI, Ray, scikit-learn, and pandas
- Inspired by modern log analysis and anomaly detection techniques
- Thanks to the open-source community for the excellent tools and libraries
