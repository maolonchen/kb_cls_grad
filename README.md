# **haul** Data Management Platform

A unified data management platform for **knowledge base management** and **specification interpretation**, powered by LLMs and vector databases. Designed for data security classification and grading in the telecommunications domain.

## Features

### Knowledge Base Management
- **Multi-format document parsing** — Supports PDF, Word, Excel, TXT, and CSV
- **AI-powered document classification** — Uses LLM to classify documents as narrative or structured data
- **Intelligent chunking** — Different strategies for narrative text (heading-based) and structured data (LLM-guided)
- **Vector search** — Embeds chunks and stores in Milvus for similarity retrieval
- **Data recognition** — AI-driven table/field-level classification and grading
- **Data element management** — CRUD operations for data elements with batch matching

### Specification Interpretation
- **Excel rule parsing** — Reads data classification rules and grading specifications from Excel files
- **Multi-step pipeline** — Tree extraction, entity/feature extraction, similarity comparison, and grading
- **Kafka integration** — Outputs results to Kafka for downstream consumption
- **Grading assignment** — Maps data to core/important/general tiers with applicable scene types


## Prerequisites

- **Python 3.10+**
- **uv** package manager
- **Milvus 2.6.8** (vector database)
- **External AI services:**
  - MinerU — Document parsing (PDF to Markdown)
  - LLM — Chat LLM for classification and chunking
  - Embed — Text embedding model

## Quick Start

### 1. Install dependencies

```bash
uv sync
```

### 2. Start Milvus

```bash
docker compose up -d
```

### 3. Configure services

Edit `app/core/config.py` to set your service URLs:

| Config | Default | Description |
|---|---|---|
| Chat LLM | `0.0.0.0:8000` | LLM endpoint |
| Embedding | `0.0.0.0:9998` | Embed endpoint |
| MinerU | `0.0.0.0:8003` | Document parsing service |
| Milvus | `0.0.0.?:19530` | Vector database |

### 4. Run the server

```bash
# Production
bash manage_interp_kb.sh start

# Development (with debug logging)
bash manage_interp_kb.sh dev

# Or directly
uv run python main.py
```

The server starts at `http://localhost:<?>`. API docs available at `/docs`.

## API Endpoints

### Knowledge Base

| Method | Description |
|---|---|
| POST | Upload files to knowledge base |
| POST | Delete files from knowledge base |
| POST | Rebuild vector database |
| POST | AI data recognition (table/field level) |
| POST | AI file-level recognition |
| POST | Get classification info |
| POST | Get data element info |
| GET | Knowledge base size |
| GET | Health check |

### Specification Interpretation

| Method | Description |
|---|---|
| POST | Create interpretation task (full pipeline) |
| POST | Create task (up to final JSONL) |
| GET | Get task results |
| POST | Batch data element matching |

## Processing Pipeline

```
File Upload → Format Conversion → Markdown Cleanup → LLM Classification
    → Intelligent Chunking → Vector Embedding → Milvus Storage → Kafka Notification
```

## Management Commands

```bash
bash manage_interp_kb.sh start     # Start server
bash manage_interp_kb.sh stop      # Stop server
bash manage_interp_kb.sh stop -a   # Force stop all related processes
bash manage_interp_kb.sh status    # Check server status
bash manage_interp_kb.sh logs      # View logs
bash manage_interp_kb.sh dev       # Run in development mode
bash manage_interp_kb.sh clean     # Clean temporary files
```
