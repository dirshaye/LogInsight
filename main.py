from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import logging
import os

from app.core.config import settings
from app.api import logs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Parallel Log Analyzer with FastAPI + Ray for anomaly detection in large-scale logs",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(
    logs.router,
    prefix=settings.api_prefix + "/logs",
    tags=["logs"]
)


@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint with HTML dashboard."""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>ParalogX - Parallel Log Analyzer</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                margin: 0;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                min-height: 100vh;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
                background: rgba(255, 255, 255, 0.1);
                padding: 30px;
                border-radius: 15px;
                backdrop-filter: blur(10px);
            }
            h1 {
                text-align: center;
                margin-bottom: 10px;
                font-size: 2.5em;
            }
            .subtitle {
                text-align: center;
                margin-bottom: 40px;
                opacity: 0.9;
                font-size: 1.2em;
            }
            .feature-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                gap: 20px;
                margin-bottom: 40px;
            }
            .feature-card {
                background: rgba(255, 255, 255, 0.1);
                padding: 20px;
                border-radius: 10px;
                border: 1px solid rgba(255, 255, 255, 0.2);
            }
            .feature-card h3 {
                margin-top: 0;
                color: #ffd700;
            }
            .api-section {
                background: rgba(255, 255, 255, 0.1);
                padding: 20px;
                border-radius: 10px;
                margin-bottom: 20px;
            }
            .endpoint {
                background: rgba(0, 0, 0, 0.3);
                padding: 10px;
                margin: 10px 0;
                border-radius: 5px;
                font-family: monospace;
            }
            .method {
                display: inline-block;
                padding: 3px 8px;
                border-radius: 3px;
                font-weight: bold;
                margin-right: 10px;
            }
            .get { background-color: #61affe; }
            .post { background-color: #49cc90; }
            .button {
                display: inline-block;
                padding: 10px 20px;
                background: #ffd700;
                color: #333;
                text-decoration: none;
                border-radius: 5px;
                font-weight: bold;
                margin: 5px;
                transition: background 0.3s;
            }
            .button:hover {
                background: #ffed4a;
            }
            .demo-section {
                text-align: center;
                margin-top: 30px;
            }
            .status-indicator {
                display: inline-block;
                width: 10px;
                height: 10px;
                border-radius: 50%;
                margin-right: 5px;
            }
            .status-online { background-color: #4CAF50; }
            .status-offline { background-color: #f44336; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🚀 ParalogX</h1>
            <p class="subtitle">Parallel Log Analyzer with FastAPI + Ray</p>
            
            <div class="feature-grid">
                <div class="feature-card">
                    <h3>⚡ Distributed Processing</h3>
                    <p>Leverage Ray for distributed parallel processing of large-scale log files with automatic speedup detection.</p>
                </div>
                <div class="feature-card">
                    <h3>🔍 Anomaly Detection</h3>
                    <p>Multi-method anomaly detection using statistical analysis, pattern recognition, temporal analysis, and content similarity.</p>
                </div>
                <div class="feature-card">
                    <h3>📊 Performance Monitoring</h3>
                    <p>Real-time performance metrics and side-by-side comparison of parallel vs sequential processing.</p>
                </div>
                <div class="feature-card">
                    <h3>🔧 Format Support</h3>
                    <p>Auto-detection and parsing of multiple log formats including Apache, Nginx, Syslog, JSON, and generic formats.</p>
                </div>
            </div>

            <div class="api-section">
                <h3>🔗 API Endpoints</h3>
                <div class="endpoint">
                    <span class="method get">GET</span> /api/v1/logs/health - Health check and Ray status
                </div>
                <div class="endpoint">
                    <span class="method post">POST</span> /api/v1/logs/upload - Upload log files
                </div>
                <div class="endpoint">
                    <span class="method post">POST</span> /api/v1/logs/analyze - Analyze logs for anomalies
                </div>
                <div class="endpoint">
                    <span class="method post">POST</span> /api/v1/logs/compare-performance - Compare parallel vs sequential
                </div>
                <div class="endpoint">
                    <span class="method get">GET</span> /api/v1/logs/sample-analysis - Run demo analysis
                </div>
                <div class="endpoint">
                    <span class="method get">GET</span> /api/v1/logs/ray/status - Ray cluster status
                </div>
            </div>

            <div class="demo-section">
                <h3>🎮 Try It Out</h3>
                <p>Quick links to get started:</p>
                <a href="/docs" class="button">📖 API Documentation</a>
                <a href="/api/v1/logs/health" class="button">🔍 Health Check</a>
                <a href="/api/v1/logs/sample-analysis" class="button">🚀 Run Demo</a>
                <a href="/redoc" class="button">📚 ReDoc</a>
            </div>

            <div class="api-section">
                <h3>💡 Quick Start Example</h3>
                <p>Try the sample analysis to see ParalogX in action:</p>
                <pre style="background: rgba(0,0,0,0.3); padding: 15px; border-radius: 5px; overflow-x: auto;">
curl -X GET "http://localhost:8000/api/v1/logs/sample-analysis"
                </pre>
                <p>Or upload and analyze your own log file:</p>
                <pre style="background: rgba(0,0,0,0.3); padding: 15px; border-radius: 5px; overflow-x: auto;">
# Upload file
curl -X POST "http://localhost:8000/api/v1/logs/upload" -F "file=@your_log_file.log"

# Analyze with parallel processing
curl -X POST "http://localhost:8000/api/v1/logs/analyze" \\
  -H "Content-Type: application/json" \\
  -d '{"file_path": "/path/to/uploaded/file", "parallel": true}'
                </pre>
            </div>
        </div>

        <script>
            // Auto-refresh health status every 30 seconds
            setInterval(async () => {
                try {
                    const response = await fetch('/api/v1/logs/health');
                    const health = await response.json();
                    console.log('Health check:', health);
                } catch (error) {
                    console.error('Health check failed:', error);
                }
            }, 30000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    logger.info("Starting ParalogX application...")
    logger.info(f"Application version: {settings.app_version}")
    logger.info(f"Debug mode: {settings.debug}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("Shutting down ParalogX application...")
    # Shutdown Ray if initialized
    try:
        from app.api.logs import processor
        processor.shutdown_ray()
    except Exception as e:
        logger.error(f"Error shutting down Ray: {e}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level="info"
    )