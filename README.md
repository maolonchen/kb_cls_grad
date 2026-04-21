# KB Cls Grad - Data Classification & Grading Platform

A unified data management platform for **knowledge base management** and **specification interpretation**, powered by LLMs and vector databases. Designed for data security classification and grading in the telecommunications domain.

## Features

### Knowledge Base Management
- **Multi-format document parsing** — Supports PDF, Word, Excel, TXT, and CSV
- **AI-powered document classification** — Uses LLM (Qwen3-32B) to classify documents as narrative or structured data
- **Intelligent chunking** — Different strategies for narrative text (heading-based) and structured data (LLM-guided)
- **Vector search** — Embeds chunks via Qwen3-Embedding-8B (4096-dim) and stores in Milvus for similarity retrieval
- **Data recognition** — AI-driven table/field-level classification and grading
- **Data element management** — CRUD operations for data elements with batch matching

### Specification Interpretation
- **Excel rule parsing** — Reads data classification rules and grading specifications from Excel files
- **Multi-step pipeline** — Tree extraction, entity/feature extraction, similarity comparison, and grading
- **Kafka integration** — Outputs results to Kafka for downstream consumption
- **Grading assignment** — Maps data to core/important/general tiers with applicable scene types

## Architecture

```
kb_cls_grad/
├── main.py                                  # FastAPI entry point (port 64001)
├── app/                                     # Knowledge Base module
│   ├── core/                                # Config, logging, utils, vector client
│   ├── algorithms/                          # Classification, similarity, chunking
│   ├── processors/                          # File parsing (PDF, Word, Excel, TXT, CSV)
│   ├── schemas/                             # Pydantic request/response models
│   ├── services/                            # Business logic layer
│   ├── api/v1/endpoints/                    # REST API endpoints
│   └── core/prompts/                        # LLM prompt templates
├── interpretation_specification/            # Spec Interpretation module
│   ├── scripts/                             # Standalone FastAPI router
│   ├── config/                              # Module configuration
│   ├── src/                                 # Pipeline processors
│   └── services/                            # Supporting services
├── scripts/                                 # Utility scripts
├── data/                                    # Data storage (raw, processed, standards)
└── test/                                    # Test files
```

## Prerequisites

- **Python 3.10+**
- **uv** package manager
- **Milvus 2.6.8** (vector database)
- **External AI services:**
  - MinerU — Document parsing (PDF to Markdown)
  - Qwen3-32B — Chat LLM for classification and chunking
  - Qwen3-Embedding-8B — Text embedding model

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
| Chat LLM | `192.168.101.113:8000` | Qwen3-32B endpoint |
| Embedding | `192.168.101.113:9998` | Qwen3-Embedding-8B endpoint |
| MinerU | `192.168.101.113:8003` | Document parsing service |
| Milvus | `192.168.10.15:19530` | Vector database |

### 4. Run the server

```bash
# Production
bash manage_interp_kb.sh start

# Development (with debug logging)
bash manage_interp_kb.sh dev

# Or directly
uv run python main.py
```

The server starts at `http://localhost:64001`. API docs available at `/docs`.

## API Endpoints

### Knowledge Base

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/specification/knowledgeBase` | Upload files to knowledge base |
| POST | `/api/v1/specification/knowledgeBase/delete` | Delete files from knowledge base |
| POST | `/api/v1/specification/knowledgeBase/rebuild` | Rebuild vector database |
| POST | `/api/v1/dataRecognition` | AI data recognition (table/field level) |
| POST | `/api/v1/fileRecognition` | AI file-level recognition |
| POST | `/api/v1/specification/knowledgeBase/classification` | Get classification info |
| POST | `/api/v1/specification/knowledgeBase/dataElement` | Get data element info |
| GET | `/api/v1/specification/knowledgeBase/sizeInformation` | Knowledge base size |
| GET | `/api/v1/health` | Health check |

### Specification Interpretation

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/specification/tasks` | Create interpretation task (full pipeline) |
| POST | `/api/v1/specification/tasks/final-jsonl` | Create task (up to final JSONL) |
| GET | `/api/v1/specification/tasks/{uid}/final-jsonl` | Get task results |
| POST | `/api/v1/dataElements/match` | Batch data element matching |

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

## Tech Stack

- **FastAPI** + **Uvicorn** — Web framework and ASGI server
- **Milvus** — Vector database (with etcd + MinIO)
- **Qwen3-32B** — Large language model
- **Qwen3-Embedding-8B** — Text embedding model
- **Kafka** — Message queue (optional)
- **pandas** / **openpyxl** — Data processing
- **aiohttp** — Async HTTP client
