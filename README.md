# LogInsight

An AI-powered log processing pipeline that transforms chaotic raw logs into clean, structured data using Claude AI.

## Overview

LogInsight processes messy log files through an intelligent pipeline that normalizes timestamps, standardizes log levels, extracts service information, and removes duplicates. The system handles JSON, JSONL, and plain text log formats.

## Architecture

- **Backend**: FastAPI with modular Python components
- **Frontend**: React TypeScript interface
- **AI Processing**: Anthropic Claude API integration
- **Storage**: SQLite with hash-based deduplication
- **Performance**: Parallel processing with multiprocessing

## Features

- File upload support for multiple log formats
- Intelligent chunking for large files
- Parallel processing with configurable workers
- AI-powered log cleaning and normalization
- Duplicate detection and removal
- Web interface with processing status
- Performance benchmarking

## Setup

### Backend
```bash
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Add your Anthropic API key to `.env`:
```
ANTHROPIC_API_KEY=your_key_here
```

Start the server:
```bash
python main.py
```

### Frontend
```bash
cd frontend
npm install
npm start
```

## Usage

1. Access the web interface at http://localhost:3001
2. Upload log files through the Upload tab
3. Process files using parallel or sequential mode
4. View cleaned logs and statistics in the Logs tab
5. Run performance benchmarks in the Benchmark tab

## Technical Details

The system processes logs through a four-stage pipeline:
1. **Ingestion**: File validation and temporary storage
2. **Chunking**: Splitting large files into manageable pieces
3. **Processing**: AI-powered cleaning through Claude API
4. **Merging**: Deduplication and final storage

## Dependencies

- FastAPI 0.104.1
- Anthropic 0.34.0
- React 18.2.0
- TypeScript 4.9.5
- SQLite3 (built-in)

## License

MIT