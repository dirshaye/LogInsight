import React, { useState, useEffect } from 'react';
import './App.css';

interface FileInfo {
  file_id: string;
  original_name: string;
  file_type: string;
  size_bytes: number;
  upload_time: string;
  status: string;
}

interface ProcessingJob {
  processing_id: string;
  status: string;
  file_info: FileInfo;
  chunk_count: number;
  estimated_time_minutes: number;
}

interface CleanedLog {
  timestamp: string;
  level: string;
  message: string;
  service?: string;
  user_id?: string;
  request_id?: string;
  [key: string]: any;
}

interface CleanedLogsResponse {
  logs: CleanedLog[];
  total_count: number;
  statistics: {
    total_logs: number;
    level_distribution: { [key: string]: number };
    top_services: { [key: string]: number };
    storage_type: string;
  };
}

interface PipelineStatus {
  message: string;
  version: string;
  status: string;
  pipeline_flow: string[];
  features: {
    file_upload: boolean;
    chunking: boolean;
    parallel_processing: boolean;
    ai_cleaning: boolean;
    deduplication: boolean;
    benchmarking: boolean;
  };
  parallel_workers: number;
}

function App() {
  const [pipelineStatus, setPipelineStatus] = useState<PipelineStatus | null>(null);
  const [uploadedFiles, setUploadedFiles] = useState<FileInfo[]>([]);
  const [processingJobs, setProcessingJobs] = useState<ProcessingJob[]>([]);
  const [cleanedLogs, setCleanedLogs] = useState<CleanedLog[]>([]);
  const [statistics, setStatistics] = useState<any>(null);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [isUploading, setIsUploading] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'upload' | 'process' | 'logs' | 'benchmark'>('upload');

  const fetchPipelineStatus = async () => {
    try {
      const response = await fetch('http://localhost:8000/');
      if (response.ok) {
        const data: PipelineStatus = await response.json();
        setPipelineStatus(data);
      }
    } catch (err) {
      console.error('Failed to fetch pipeline status:', err);
    }
  };

  const fetchUploadedFiles = async () => {
    try {
      const response = await fetch('http://localhost:8000/uploaded-files');
      if (response.ok) {
        const data = await response.json();
        setUploadedFiles(data.uploaded_files || []);
      }
    } catch (err) {
      console.error('Failed to fetch uploaded files:', err);
    }
  };

  const fetchProcessingJobs = async () => {
    try {
      const response = await fetch('http://localhost:8000/processing-jobs');
      if (response.ok) {
        const data = await response.json();
        setProcessingJobs(data.processing_jobs || []);
      }
    } catch (err) {
      console.error('Failed to fetch processing jobs:', err);
    }
  };

  const fetchCleanedLogs = async () => {
    try {
      const response = await fetch('http://localhost:8000/cleaned?limit=50');
      if (response.ok) {
        const data: CleanedLogsResponse = await response.json();
        setCleanedLogs(data.logs);
        setStatistics(data.statistics);
      }
    } catch (err) {
      console.error('Failed to fetch cleaned logs:', err);
    }
  };

  useEffect(() => {
    fetchPipelineStatus();
    fetchUploadedFiles();
    fetchProcessingJobs();
    fetchCleanedLogs();
  }, []);

  const handleFileUpload = async () => {
    if (!selectedFile) return;

    setIsUploading(true);
    setError(null);

    try {
      const formData = new FormData();
      formData.append('file', selectedFile);

      const response = await fetch('http://localhost:8000/upload', {
        method: 'POST',
        body: formData,
      });

      if (response.ok) {
        const data = await response.json();
        console.log('File uploaded successfully:', data);
        setSelectedFile(null);
        
        // Refresh the uploaded files list
        await fetchUploadedFiles();
        
        // Switch to process tab
        setActiveTab('process');
      } else {
        const errorData = await response.json();
        setError(errorData.detail || 'Upload failed');
      }
    } catch (err) {
      setError('Upload failed: ' + (err as Error).message);
    } finally {
      setIsUploading(false);
    }
  };

  const handleProcessFile = async (fileId: string, useParallel: boolean = true) => {
    setIsProcessing(true);
    setError(null);

    try {
      const response = await fetch('http://localhost:8000/process', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          file_id: fileId,
          use_parallel: useParallel,
          chunk_size_mb: 50
        }),
      });

      if (response.ok) {
        const data = await response.json();
        console.log('Processing completed:', data);
        
        // Refresh data
        await fetchProcessingJobs();
        await fetchCleanedLogs();
        
        // Switch to logs tab
        setActiveTab('logs');
      } else {
        const errorData = await response.json();
        setError(errorData.detail || 'Processing failed');
      }
    } catch (err) {
      setError('Processing failed: ' + (err as Error).message);
    } finally {
      setIsProcessing(false);
    }
  };

  const runBenchmark = async () => {
    try {
      const response = await fetch('http://localhost:8000/benchmark', {
        method: 'POST',
      });

      if (response.ok) {
        const data = await response.json();
        console.log('Benchmark results:', data);
        // Could display benchmark results in a modal or section
      } else {
        const errorData = await response.json();
        setError(errorData.detail || 'Benchmark failed');
      }
    } catch (err) {
      setError('Benchmark failed: ' + (err as Error).message);
    }
  };

  const formatFileSize = (bytes: number): string => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>🔍 LogInsight Agent</h1>
        <p>Log Processing Pipeline - Upload → Chunk → Clean → Merge</p>
        
        {pipelineStatus && (
          <div className="pipeline-status">
            <span>Status: {pipelineStatus.status}</span>
            <span>Version: {pipelineStatus.version}</span>
            <span>Workers: {pipelineStatus.parallel_workers}</span>
          </div>
        )}
      </header>

      <main className="main-content">
        {error && (
          <div className="error-message">
            ❌ {error}
            <button onClick={() => setError(null)}>✕</button>
          </div>
        )}

        <nav className="tab-navigation">
          <button 
            className={activeTab === 'upload' ? 'active' : ''}
            onClick={() => setActiveTab('upload')}
          >
            📁 Upload
          </button>
          <button 
            className={activeTab === 'process' ? 'active' : ''}
            onClick={() => setActiveTab('process')}
          >
            ⚡ Process
          </button>
          <button 
            className={activeTab === 'logs' ? 'active' : ''}
            onClick={() => setActiveTab('logs')}
          >
            📋 Logs
          </button>
          <button 
            className={activeTab === 'benchmark' ? 'active' : ''}
            onClick={() => setActiveTab('benchmark')}
          >
            📊 Benchmark
          </button>
        </nav>

        {/* Upload Tab */}
        {activeTab === 'upload' && (
          <section className="upload-section">
            <h2>📁 Upload Log Files</h2>
            <p>Upload raw log files (JSON, JSONL, or plain text) for processing</p>
            
            <div className="upload-area">
              <input
                type="file"
                accept=".json,.jsonl,.log,.txt"
                onChange={(e) => setSelectedFile(e.target.files?.[0] || null)}
                disabled={isUploading}
                aria-label="Select log file to upload"
                title="Select log file to upload"
              />
              
              {selectedFile && (
                <div className="file-info">
                  <p>Selected: {selectedFile.name}</p>
                  <p>Size: {formatFileSize(selectedFile.size)}</p>
                  <p>Type: {selectedFile.type || 'text/plain'}</p>
                </div>
              )}
              
              <button
                onClick={handleFileUpload}
                disabled={!selectedFile || isUploading}
                className="upload-button"
              >
                {isUploading ? '📤 Uploading...' : '📤 Upload File'}
              </button>
            </div>

            <div className="uploaded-files">
              <h3>Uploaded Files ({uploadedFiles.length})</h3>
              {uploadedFiles.length === 0 ? (
                <p>No files uploaded yet</p>
              ) : (
                <ul className="file-list">
                  {uploadedFiles.map((file) => (
                    <li key={file.file_id} className="file-item">
                      <span className="file-name">{file.original_name}</span>
                      <span className="file-type">{file.file_type}</span>
                      <span className="file-size">{formatFileSize(file.size_bytes)}</span>
                      <span className="file-status">{file.status}</span>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </section>
        )}

        {/* Process Tab */}
        {activeTab === 'process' && (
          <section className="process-section">
            <h2>⚡ Process Files</h2>
            <p>Chunk files and process them through Claude AI for cleaning and normalization</p>
            
            <div className="files-to-process">
              <h3>Available Files for Processing</h3>
              {uploadedFiles.length === 0 ? (
                <p>No files available. Upload files first.</p>
              ) : (
                <div className="process-files">
                  {uploadedFiles.map((file) => (
                    <div key={file.file_id} className="process-file-item">
                      <div className="file-details">
                        <strong>{file.original_name}</strong>
                        <span>Type: {file.file_type}</span>
                        <span>Size: {formatFileSize(file.size_bytes)}</span>
                      </div>
                      
                      <div className="process-actions">
                        <button
                          onClick={() => handleProcessFile(file.file_id, true)}
                          disabled={isProcessing}
                          className="process-button parallel"
                        >
                          🚀 Process (Parallel)
                        </button>
                        
                        <button
                          onClick={() => handleProcessFile(file.file_id, false)}
                          disabled={isProcessing}
                          className="process-button sequential"
                        >
                          🐌 Process (Sequential)
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>

            <div className="processing-jobs">
              <h3>Processing Jobs ({processingJobs.length})</h3>
              {processingJobs.length === 0 ? (
                <p>No processing jobs yet</p>
              ) : (
                <ul className="job-list">
                  {processingJobs.map((job) => (
                    <li key={job.processing_id} className="job-item">
                      <div className="job-info">
                        <strong>{job.file_info.original_name}</strong>
                        <span>Status: {job.status}</span>
                        <span>Chunks: {job.chunk_count}</span>
                        <span>Time: {job.estimated_time_minutes ? job.estimated_time_minutes.toFixed(2) : '0.00'} min</span>
                      </div>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </section>
        )}

        {/* Logs Tab */}
        {activeTab === 'logs' && (
          <section className="logs-section">
            <h2>📋 Cleaned Logs</h2>
            <p>View the cleaned and normalized log entries</p>
            
            {statistics && (
              <div className="log-statistics">
                <h3>Statistics</h3>
                <div className="stats-grid">
                  <div className="stat-item">
                    <strong>Total Logs:</strong> {statistics.total_logs}
                  </div>
                  <div className="stat-item">
                    <strong>Storage:</strong> {statistics.storage_type}
                  </div>
                  
                  {statistics.level_distribution && (
                    <div className="stat-item">
                      <strong>Levels:</strong>
                      {Object.entries(statistics.level_distribution).map(([level, count]) => (
                        <span key={level} className={`level-badge ${level.toLowerCase()}`}>
                          {level}: {count as number}
                        </span>
                      ))}
                    </div>
                  )}
                  
                  {statistics.top_services && (
                    <div className="stat-item">
                      <strong>Top Services:</strong>
                      {Object.entries(statistics.top_services).slice(0, 5).map(([service, count]) => (
                        <span key={service} className="service-badge">
                          {service}: {count as number}
                        </span>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            )}

            <div className="logs-table">
              <h3>Log Entries ({cleanedLogs.length})</h3>
              {cleanedLogs.length === 0 ? (
                <p>No cleaned logs available. Process some files first.</p>
              ) : (
                <table className="logs-table-view">
                  <thead>
                    <tr>
                      <th>Timestamp</th>
                      <th>Level</th>
                      <th>Service</th>
                      <th>Message</th>
                    </tr>
                  </thead>
                  <tbody>
                    {cleanedLogs.slice(0, 50).map((log, index) => (
                      <tr key={index} className={`log-row ${log.level.toLowerCase()}`}>
                        <td className="timestamp">{new Date(log.timestamp).toLocaleString()}</td>
                        <td className={`level ${log.level.toLowerCase()}`}>{log.level}</td>
                        <td className="service">{log.service || 'unknown'}</td>
                        <td className="message">{log.message}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </section>
        )}

        {/* Benchmark Tab */}
        {activeTab === 'benchmark' && (
          <section className="benchmark-section">
            <h2>📊 Performance Benchmark</h2>
            <p>Compare parallel vs sequential processing performance</p>
            
            <div className="benchmark-controls">
              <button
                onClick={runBenchmark}
                className="benchmark-button"
              >
                🏃‍♂️ Run Benchmark
              </button>
            </div>

            {pipelineStatus && (
              <div className="pipeline-info">
                <h3>Pipeline Information</h3>
                <div className="info-grid">
                  <div className="info-item">
                    <strong>Parallel Workers:</strong> {pipelineStatus.parallel_workers}
                  </div>
                  <div className="info-item">
                    <strong>Features:</strong>
                    <ul>
                      {pipelineStatus.pipeline_flow.map((step, index) => (
                        <li key={index}>{step}</li>
                      ))}
                    </ul>
                  </div>
                  <div className="info-item">
                    <strong>Capabilities:</strong>
                    <div className="features">
                      {Object.entries(pipelineStatus.features).map(([feature, enabled]) => (
                        <span key={feature} className={`feature ${enabled ? 'enabled' : 'disabled'}`}>
                          {enabled ? '✅' : '❌'} {feature.replace('_', ' ')}
                        </span>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            )}
          </section>
        )}
      </main>
    </div>
  );
}

export default App;
