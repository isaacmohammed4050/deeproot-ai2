import os
import json
import gzip
import logging
from datetime import datetime, timezone
from typing import List

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# CONFIG

ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("deeproot-agent")

app = FastAPI(title="Deeproot-Agent", version="1.0")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ELASTICSEARCH CLIENT

class ESClient:
def **init**(self, host: str):
self.host = host.rstrip("/")
self.client = httpx.AsyncClient(timeout=30.0)

```
async def list_indices(self) -> List[str]:
    try:
        r = await self.client.get(f"{self.host}/_cat/indices?format=json")
        data = r.json()
        return [i["index"] for i in data]
    except Exception as e:
        logger.error(f"Index list error: {e}")
        return []

async def search_all(self, query: dict, size: int = 100) -> dict:
    try:
        r = await self.client.post(
            f"{self.host}/_search",
            json={"query": query, "size": size}
        )
        return r.json()
    except Exception as e:
        logger.error(f"Search error: {e}")
        return {"hits": {"hits": [], "total": {"value": 0}}}

async def bulk_index(self, index: str, docs: list):
    if not docs:
        return

    body = ""
    for d in docs:
        body += json.dumps({"index": {"_index": index}}) + "\n"
        body += json.dumps(d) + "\n"

    try:
        await self.client.post(
            f"{self.host}/_bulk",
            content=body,
            headers={"Content-Type": "application/x-ndjson"}
        )
    except Exception as e:
        logger.error(f"Bulk index error: {e}")
```

es = ESClient(ES_HOST)

# OTEL PARSERS

def parse_logs(payload: dict) -> list:
docs = []

```
for rl in payload.get("resourceLogs", []):
    service = "unknown"

    for attr in rl.get("resource", {}).get("attributes", []):
        if attr.get("key") == "service.name":
            service = attr["value"].get("stringValue", "unknown")

    for sl in rl.get("scopeLogs", []):
        for lr in sl.get("logRecords", []):
            docs.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "service_name": service,
                "severity": lr.get("severityText", "INFO"),
                "body": str(lr.get("body", {}))
            })

return docs
```

def parse_traces(payload: dict) -> list:
docs = []

```
for rs in payload.get("resourceSpans", []):
    service = "unknown"

    for attr in rs.get("resource", {}).get("attributes", []):
        if attr.get("key") == "service.name":
            service = attr["value"].get("stringValue", "unknown")

    for ss in rs.get("scopeSpans", []):
        for span in ss.get("spans", []):
            docs.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "service_name": service,
                "duration_ms": 1,
                "status_code": span.get("status", {}).get("code", "")
            })

return docs
```

# AGENT

class Agent:

```
async def ask_llm(self, prompt: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            r = await client.post(
                OLLAMA_URL,
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "stream": False
                }
            )
            return r.json().get("response", "")
    except Exception as e:
        return f"LLM error: {e}"

async def run(self, user_query: str) -> dict:

    indices = await es.list_indices()

    query = {
        "multi_match": {
            "query": user_query,
            "fields": ["*", "body", "message"]
        }
    }

    raw = await es.search_all(query, size=100)

    docs = []
    for h in raw.get("hits", {}).get("hits", []):
        src = h.get("_source", {})
        docs.append({
            "service": src.get("service_name") or "unknown",
            "message": src.get("body") or src.get("message") or "",
            "severity": src.get("severity") or "INFO",
            "timestamp": src.get("timestamp") or ""
        })

    if "show" in user_query.lower():
        return {
            "indices": indices,
            "sample_data": docs[:10]
        }

    prompt = f"""
```

You are Deeproot-Agent.

User Query: {user_query}

Logs:
{docs[:20]}

Give RCA and summary.
"""

```
    analysis = await self.ask_llm(prompt)

    return {
        "query": user_query,
        "docs_found": len(docs),
        "analysis": analysis
    }
```

agent = Agent()

# API

class QueryRequest(BaseModel):
query: str

@app.post("/api/query")
async def query_api(req: QueryRequest):
return await agent.run(req.query)

@app.get("/api/indices")
async def indices():
return {"indices": await es.list_indices()}

@app.get("/api/debug")
async def debug():
return await es.search_all({"match_all": {}}, 10)

# OTEL INGEST

@app.post("/v1/logs")
async def logs(request: Request):
raw = await request.body()

```
if request.headers.get("content-encoding") == "gzip":
    raw = gzip.decompress(raw)

payload = json.loads(raw.decode("utf-8"))
docs = parse_logs(payload)

await es.bulk_index("otel-logs", docs)

return {"ingested": len(docs)}
```

@app.post("/v1/traces")
async def traces(request: Request):
raw = await request.body()

```
if request.headers.get("content-encoding") == "gzip":
    raw = gzip.decompress(raw)

payload = json.loads(raw.decode("utf-8"))
docs = parse_traces(payload)

await es.bulk_index("otel-traces", docs)

return {"ingested": len(docs)}
```

# UI

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
return templates.TemplateResponse(
"index.html",
{"request": request, "app_name": "Deeproot-Agent 🧠"}
)
