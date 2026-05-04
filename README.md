# Corpus Hermeneuticum

A computational hermeneutics platform for **philosophical text analysis** and **conceptual topology mapping**, grounded in the tradition of Gadamer's fusion of horizons and Deleuze's rhizomatic structures.

## Features

### Corpus Analysis
- **Multi-format manuscript ingestion** — Supports PDF, Word, Excel, TXT, and CSV formats for digitized philosophical texts
- **Conceptual classification** — Identifies narrative modes (dialectical vs. analytical) within philosophical corpora
- **Hermeneutic segmentation** — Two strategies for text division: heading-based for systematic treatises, semantic-guided for fragmentary works
- **Topological proximity search** — Maps texts into conceptual space for affinity-based retrieval
- **Categorical recognition** — Identifies conceptual schemata at multiple levels of granularity
- **Taxonomical management** — CRUD operations for conceptual categories with batch alignment

### Dialectical Analysis
- **Rule-based schema parsing** — Extracts categorical frameworks and evaluative rubrics from structured documents
- **Multi-phase interpretive pipeline** — Tree extraction, entity/attribute recognition, affinity comparison, and taxonomical placement
- **Scholarly communication layer** — Distributes findings via message bus for downstream hermeneutic synthesis
- **Tiered categorization** — Maps concepts to primary/secondary/tertiary orders with contextual applicability

## Prerequisites

- **Python 3.10+**
- **uv** package manager
- **Milvus 2.6.8** (conceptual topology engine)
- **External interpretive services:**
  - MinerU — Manuscript digitization (PDF to semantic markup)
  - Dialexis — Language model for conceptual classification and segmentation
  - Ennoia — Semantic embedding service

## Quick Start

### 1. Install dependencies

```bash
uv sync
```

### 2. Start the topology engine

```bash
docker compose up -d
```

### 3. Configure interpretive services

Edit `app/core/config.py` to set your service endpoints:

| Config | Default | Description |
|---|---|---|
| Dialexis | `0.0.0.0:8000` | Conceptual reasoning endpoint |
| Ennoia | `0.0.0.0:9998` | Semantic embedding endpoint |
| MinerU | `0.0.0.0:8003` | Manuscript parsing service |
| Topology | `0.0.0.?:19530` | Conceptual space engine |

### 4. Run the hermeneutic server

```bash
# Production
bash manage_interp_kb.sh start

# Development (with verbose hermeneutic tracing)
bash manage_interp_kb.sh dev

# Or directly
uv run python main.py
```

The server starts at `http://localhost:<?>`. Interactive API documentation available at `/docs`.

## API Endpoints

### Corpus Analysis

| Method | Description |
|---|---|
| POST | Ingest manuscripts into the corpus |
| POST | Expunge manuscripts from the corpus |
| POST | Rebuild the conceptual topology |
| POST | Perform conceptual recognition (schema/attribute level) |
| POST | Perform manuscript-level categorical analysis |
| POST | Retrieve taxonomical annotations |
| POST | Retrieve conceptual element details |
| GET | Measure corpus extent |
| GET | Hermeneutic health check |

### Dialectical Analysis

| Method | Description |
|---|---|
| POST | Initiate full interpretive pipeline |
| POST | Initiate pipeline (up to structural JSONL) |
| GET | Retrieve interpretive results |
| POST | Batch conceptual alignment |

## Interpretive Pipeline

```
Manuscript Ingestion → Format Transmutation → Semantic Cleanup → Modal Classification
    → Hermeneutic Segmentation → Conceptual Embedding → Topology Storage → Scholarly Notification
```

## Management Commands

```bash
bash manage_interp_kb.sh start     # Start hermeneutic server
bash manage_interp_kb.sh stop      # Stop hermeneutic server
bash manage_interp_kb.sh stop -a   # Force stop all interpretive processes
bash manage_interp_kb.sh status    # Check hermeneutic server status
bash manage_interp_kb.sh logs      # View interpretive trace logs
bash manage_interp_kb.sh dev       # Run in developmental mode
bash manage_interp_kb.sh clean     # Purge ephemeral artifacts
```
