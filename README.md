# OTel Observability Agent

AI-powered observability agent for Kubernetes. Ingests OpenTelemetry logs & traces, stores them in Elasticsearch, and provides a natural language query interface powered by Ollama/Mistral for anomaly detection, root cause analysis, and intelligent observability.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Kubernetes Cluster                          │
│                                                                 │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                       │
│  │ Node.js  │ │  Java    │ │  React   │  ← Your applications  │
│  │  App     │ │  App     │ │  App     │    (instrumented)      │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘                       │
│       │             │            │                              │
│       ▼             ▼            ▼                              │
│  ┌──────────────────────────────────────┐                      │
│  │     OTel Collector (DaemonSet)       │                      │
│  │  Receives logs + traces via OTLP     │                      │
│  └──────────────────┬───────────────────┘                      │
│                     │ OTLP/HTTP                                │
│                     ▼                                          │
│  ┌──────────────────────────────────────┐                      │
│  │        OTel Agent (this app)         │                      │
│  │  ┌──────────┐  ┌──────────────────┐  │                      │
│  │  │ OTLP     │  │  Query Engine    │  │                      │
│  │  │ Receiver │  │  (NL → ES → AI)  │  │                      │
│  │  └────┬─────┘  └───────┬──────────┘  │                      │
│  │       │                │             │                      │
│  │       ▼                ▼             │                      │
│  │  ┌──────────┐  ┌──────────────────┐  │                      │
│  │  │  Elastic │  │  Ollama/Mistral  │  │                      │
│  │  │  search  │  │  (LLM Analysis)  │  │                      │
│  │  └──────────┘  └──────────────────┘  │                      │
│  └──────────────────────────────────────┘                      │
│                     │                                          │
│                     ▼                                          │
│             ┌──────────────┐                                   │
│             │  Web UI      │ ← Ask questions in plain English  │
│             │  (port 8080) │                                   │
│             └──────────────┘                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Features

- **OTLP HTTP Receiver**: Accepts logs (`/v1/logs`) and traces (`/v1/traces`) from otel-collector
- **Elasticsearch Storage**: Separate indices for logs (`otel-logs`) and traces (`otel-traces`) with optimized mappings
- **Natural Language Query**: Ask questions like "Is there any spike in user-management-service?"
- **AI-Powered Analysis**: Ollama/Mistral performs anomaly detection, RCA, and generates human-readable insights
- **WebSocket Streaming**: Real-time query progress updates
- **Service Discovery**: Auto-detects services from ingested telemetry

## Quick Start

### 1. Build & Push Docker Image

```bash
docker build -t your-registry/otel-agent:latest .
docker push your-registry/otel-agent:latest
```

### 2. Update K8s Manifests

Edit `k8s/deployment.yaml`:
- Set your Docker image registry in the Deployment spec
- Update the Ingress hostname
- Adjust ConfigMap values if your ES/Ollama endpoints differ

### 3. Deploy

```bash
# Create namespace and deploy Elasticsearch
kubectl apply -f k8s/elasticsearch.yaml

# Wait for ES to be ready
kubectl wait --for=condition=ready pod -l app=elasticsearch -n observability --timeout=120s

# Deploy the agent
kubectl apply -f k8s/deployment.yaml
```

### 4. Configure OTel Collector

Merge `k8s/otel-collector-config.yaml` into your existing collector configuration. The key addition is two `otlphttp` exporters pointing to the agent's `/v1/logs` and `/v1/traces` endpoints.

### 5. Access the UI

```bash
# Port-forward for quick access
kubectl port-forward svc/otel-agent 8080:8080 -n observability

# Open http://localhost:8080
```

## Configuration

All configuration is via environment variables (set in the ConfigMap):

| Variable | Default | Description |
|----------|---------|-------------|
| `ES_HOST` | `http://elasticsearch:9200` | Elasticsearch endpoint |
| `ES_LOG_INDEX` | `otel-logs` | Index name for logs |
| `ES_TRACE_INDEX` | `otel-traces` | Index name for traces |
| `OLLAMA_URL` | `http://ollama:11434/api/generate` | Ollama API endpoint |
| `OLLAMA_MODEL` | `mistral` | LLM model name |
| `LOG_LEVEL` | `INFO` | Application log level |

## Example Queries

- "Is there any spike in user-management-service?"
- "Show me all errors in the last 2 hours"
- "Why is the payment-service slow?"
- "Which service has the highest error rate?"
- "Are there any 5xx errors in the API gateway?"
- "Do a root cause analysis on the order-service failures"

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Web UI |
| `/v1/logs` | POST | OTLP log receiver |
| `/v1/traces` | POST | OTLP trace receiver |
| `/api/query` | POST | Natural language query (JSON: `{"query": "..."}`) |
| `/ws/query` | WS | WebSocket query with streaming updates |
| `/api/services` | GET | List all discovered services |
| `/health` | GET | Health check |

## Security Notes

- Container runs as non-root user
- Read-only root filesystem
- All capabilities dropped
- No privilege escalation
- Add NetworkPolicy to restrict traffic in production
- Add ES authentication if using X-Pack Security
