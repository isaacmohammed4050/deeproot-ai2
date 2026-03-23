"""
DeepRoot AI - AI-powered observability agent for Kubernetes workloads.

Ingests OpenTelemetry logs & traces from otel-collector, stores in Elasticsearch,
and provides a natural language query interface powered by Ollama/Mistral for
anomaly detection, RCA, and intelligent observability.
"""

import os
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

# ─── Configuration ────────────────────────────────────────────────────────────
ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
ES_LOG_INDEX = os.getenv("ES_LOG_INDEX", "otel-logs")
ES_TRACE_INDEX = os.getenv("ES_TRACE_INDEX", "otel-traces")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("deeproot-ai")

app = FastAPI(title="DeepRoot AI", version="1.0.0", description="AI-powered observability agent")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ─── Elasticsearch Index Templates ────────────────────────────────────────────

LOG_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "timestamp": {"type": "date"},
            "service_name": {"type": "keyword"},
            "severity": {"type": "keyword"},
            "body": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 512}}},
            "resource_attributes": {"type": "object", "enabled": True},
            "log_attributes": {"type": "object", "enabled": True},
            "trace_id": {"type": "keyword"},
            "span_id": {"type": "keyword"},
            "k8s_namespace": {"type": "keyword"},
            "k8s_pod_name": {"type": "keyword"},
            "k8s_container_name": {"type": "keyword"},
        }
    },
    "settings": {
        "number_of_shards": 2,
        "number_of_replicas": 1,
        "index.refresh_interval": "5s",
    },
}

TRACE_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "timestamp": {"type": "date"},
            "service_name": {"type": "keyword"},
            "span_name": {"type": "keyword"},
            "trace_id": {"type": "keyword"},
            "span_id": {"type": "keyword"},
            "parent_span_id": {"type": "keyword"},
            "duration_ms": {"type": "float"},
            "status_code": {"type": "keyword"},
            "status_message": {"type": "text"},
            "span_kind": {"type": "keyword"},
            "attributes": {"type": "object", "enabled": True},
            "events": {"type": "nested"},
            "resource_attributes": {"type": "object", "enabled": True},
            "http_method": {"type": "keyword"},
            "http_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 512}}},
            "http_status_code": {"type": "integer"},
            "k8s_namespace": {"type": "keyword"},
            "k8s_pod_name": {"type": "keyword"},
        }
    },
    "settings": {
        "number_of_shards": 2,
        "number_of_replicas": 1,
        "index.refresh_interval": "5s",
    },
}


# ─── Elasticsearch Client ─────────────────────────────────────────────────────

class ESClient:
    """Async Elasticsearch client for log & trace operations."""

    def __init__(self, host: str):
        self.host = host.rstrip("/")
        self.client = httpx.AsyncClient(timeout=30.0)

    async def health(self) -> dict:
        try:
            r = await self.client.get(f"{self.host}/_cluster/health")
            return r.json()
        except Exception as e:
            logger.error(f"ES health check failed: {e}")
            return {"status": "unreachable", "error": str(e)}

    async def ensure_index(self, index: str, mapping: dict):
        """Create index if it doesn't exist. Retries up to 3 times on connection failure."""
        for attempt in range(3):
            try:
                r = await self.client.head(f"{self.host}/{index}")
                if r.status_code == 404:
                    r = await self.client.put(
                        f"{self.host}/{index}",
                        json=mapping,
                        headers={"Content-Type": "application/json"},
                    )
                    if r.status_code in (200, 201):
                        logger.info(f"✅ Created index '{index}' successfully")
                    else:
                        logger.error(f"❌ Failed to create index '{index}': {r.status_code} {r.text}")
                elif r.status_code == 200:
                    logger.info(f"✅ Index '{index}' already exists")
                    # Verify doc count
                    count_r = await self.client.get(f"{self.host}/{index}/_count")
                    if count_r.status_code == 200:
                        doc_count = count_r.json().get("count", 0)
                        logger.info(f"   Index '{index}' has {doc_count} documents")
                return
            except Exception as e:
                logger.warning(f"ES connection attempt {attempt+1}/3 for index '{index}': {e}")
                if attempt < 2:
                    import asyncio
                    await asyncio.sleep(5)
        logger.error(f"❌ Failed to ensure index '{index}' after 3 attempts")

    async def index_count(self, index: str) -> int:
        """Get document count for an index."""
        try:
            r = await self.client.get(
                f"{self.host}/{index}/_count",
                headers={"Content-Type": "application/json"},
            )
            return r.json().get("count", 0)
        except Exception:
            return 0

    async def search(self, index: str, query: dict, size: int = 100) -> dict:
        try:
            body = {"query": query, "size": size}
            # Only add sort if timestamp field exists in query context
            body["sort"] = [{"timestamp": {"order": "desc", "unmapped_type": "date"}}]
            r = await self.client.post(
                f"{self.host}/{index}/_search",
                json=body,
                headers={"Content-Type": "application/json"},
            )
            result = r.json()
            # Log for debugging
            hit_count = result.get("hits", {}).get("total", {}).get("value", 0)
            logger.debug(f"Search on '{index}': {hit_count} hits")
            if r.status_code != 200:
                logger.error(f"ES search error on '{index}': {r.status_code} - {r.text[:500]}")
                return {"hits": {"hits": [], "total": {"value": 0}}}
            return result
        except Exception as e:
            logger.error(f"ES search failed on '{index}': {e}")
            return {"hits": {"hits": [], "total": {"value": 0}}}

    async def aggregate(self, index: str, body: dict) -> dict:
        try:
            r = await self.client.post(
                f"{self.host}/{index}/_search",
                json=body,
                headers={"Content-Type": "application/json"},
            )
            return r.json()
        except Exception as e:
            logger.error(f"ES aggregation failed: {e}")
            return {}

    async def bulk_index(self, index: str, docs: list[dict]):
        if not docs:
            return
        bulk_body = ""
        for doc in docs:
            bulk_body += json.dumps({"index": {"_index": index}}) + "\n"
            bulk_body += json.dumps(doc) + "\n"
        try:
            r = await self.client.post(
                f"{self.host}/_bulk",
                content=bulk_body,
                headers={"Content-Type": "application/x-ndjson"},
            )
            resp = r.json()
            if resp.get("errors"):
                error_items = [i for i in resp["items"] if "error" in i.get("index", {})]
                logger.warning(f"Bulk index had {len(error_items)} errors")
        except Exception as e:
            logger.error(f"Bulk index failed: {e}")


es = ESClient(ES_HOST)


# ─── OTLP Receiver (HTTP/JSON) ───────────────────────────────────────────────

def parse_otlp_logs(payload: dict) -> list[dict]:
    """Parse OTLP log export payload into flat ES documents."""
    docs = []
    for rl in payload.get("resourceLogs", []):
        resource_attrs = _flatten_attrs(rl.get("resource", {}).get("attributes", []))
        service_name = resource_attrs.get("service.name", "unknown")
        k8s_ns = resource_attrs.get("k8s.namespace.name", "")
        k8s_pod = resource_attrs.get("k8s.pod.name", "")
        k8s_container = resource_attrs.get("k8s.container.name", "")
        for sl in rl.get("scopeLogs", []):
            for lr in sl.get("logRecords", []):
                ts = _nano_to_iso(lr.get("timeUnixNano", "0"))
                docs.append({
                    "timestamp": ts,
                    "service_name": service_name,
                    "severity": lr.get("severityText", "UNSPECIFIED"),
                    "body": _extract_body(lr.get("body", {})),
                    "resource_attributes": resource_attrs,
                    "log_attributes": _flatten_attrs(lr.get("attributes", [])),
                    "trace_id": lr.get("traceId", ""),
                    "span_id": lr.get("spanId", ""),
                    "k8s_namespace": k8s_ns,
                    "k8s_pod_name": k8s_pod,
                    "k8s_container_name": k8s_container,
                })
    return docs


def parse_otlp_traces(payload: dict) -> list[dict]:
    """Parse OTLP trace export payload into flat ES documents."""
    docs = []
    for rt in payload.get("resourceSpans", []):
        resource_attrs = _flatten_attrs(rt.get("resource", {}).get("attributes", []))
        service_name = resource_attrs.get("service.name", "unknown")
        k8s_ns = resource_attrs.get("k8s.namespace.name", "")
        k8s_pod = resource_attrs.get("k8s.pod.name", "")
        for ss in rt.get("scopeSpans", []):
            for span in ss.get("spans", []):
                start = int(span.get("startTimeUnixNano", "0"))
                end = int(span.get("endTimeUnixNano", "0"))
                duration_ms = (end - start) / 1_000_000 if start and end else 0
                span_attrs = _flatten_attrs(span.get("attributes", []))
                docs.append({
                    "timestamp": _nano_to_iso(span.get("startTimeUnixNano", "0")),
                    "service_name": service_name,
                    "span_name": span.get("name", ""),
                    "trace_id": span.get("traceId", ""),
                    "span_id": span.get("spanId", ""),
                    "parent_span_id": span.get("parentSpanId", ""),
                    "duration_ms": round(duration_ms, 3),
                    "status_code": span.get("status", {}).get("code", "UNSET"),
                    "status_message": span.get("status", {}).get("message", ""),
                    "span_kind": _span_kind(span.get("kind", 0)),
                    "attributes": span_attrs,
                    "events": span.get("events", []),
                    "resource_attributes": resource_attrs,
                    "http_method": span_attrs.get("http.method", ""),
                    "http_url": span_attrs.get("http.url", span_attrs.get("http.target", "")),
                    "http_status_code": int(span_attrs.get("http.status_code", 0) or 0),
                    "k8s_namespace": k8s_ns,
                    "k8s_pod_name": k8s_pod,
                })
    return docs


def _flatten_attrs(attrs: list) -> dict:
    result = {}
    for a in attrs:
        key = a.get("key", "")
        val = a.get("value", {})
        if "stringValue" in val:
            result[key] = val["stringValue"]
        elif "intValue" in val:
            result[key] = int(val["intValue"])
        elif "doubleValue" in val:
            result[key] = float(val["doubleValue"])
        elif "boolValue" in val:
            result[key] = val["boolValue"]
        elif "arrayValue" in val:
            result[key] = str(val["arrayValue"])
        else:
            result[key] = str(val)
    return result


def _nano_to_iso(nano_str: str) -> str:
    try:
        ns = int(nano_str)
        dt = datetime.fromtimestamp(ns / 1e9, tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _extract_body(body: dict) -> str:
    if "stringValue" in body:
        return body["stringValue"]
    return json.dumps(body)


def _span_kind(kind_int: int) -> str:
    return {0: "UNSPECIFIED", 1: "INTERNAL", 2: "SERVER", 3: "CLIENT", 4: "PRODUCER", 5: "CONSUMER"}.get(
        kind_int, "UNKNOWN"
    )


# ─── LLM Agent (Ollama / Mistral) ────────────────────────────────────────────

class ObservabilityAgent:
    """
    The core AI agent that:
    1. Understands natural language queries about services
    2. Generates ES queries to fetch relevant logs/traces
    3. Analyzes the data for anomalies, RCA, patterns
    4. Returns human-readable insights
    """

    def __init__(self):
        self.ollama_url = OLLAMA_URL
        self.model = OLLAMA_MODEL

    async def ask_llm(self, prompt: str, stream: bool = False) -> str:
        """Send a prompt to Ollama Mistral and get a response."""
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                r = await client.post(
                    self.ollama_url,
                    json={"model": self.model, "prompt": prompt, "stream": False},
                )
                data = r.json()
                return data.get("response", "").strip()
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return f"[LLM Error] Could not reach Ollama: {e}"

    async def process_query(self, user_query: str) -> dict:
        """
        Full agent pipeline:
        1. Understand intent → extract service name, time range, query type
        2. Build ES queries for logs + traces
        3. Execute queries
        4. Feed results to LLM for analysis
        5. Return structured answer
        """
        # Step 1: Intent extraction via LLM
        intent = await self._extract_intent(user_query)
        logger.info(f"Extracted intent: {intent}")

        # Step 2: Build and execute ES queries
        log_results = await self._query_logs(intent)
        trace_results = await self._query_traces(intent)

        log_count = log_results.get("hits", {}).get("total", {}).get("value", 0)
        trace_count = trace_results.get("hits", {}).get("total", {}).get("value", 0)

        # Step 3: Get aggregation stats
        stats = await self._get_service_stats(intent)

        # Step 4: Prepare context for LLM analysis
        log_samples = self._extract_log_samples(log_results, max_samples=30)
        trace_samples = self._extract_trace_samples(trace_results, max_samples=30)

        # Step 5: LLM-powered analysis
        analysis = await self._analyze_data(user_query, intent, log_samples, trace_samples, stats)

        return {
            "query": user_query,
            "intent": intent,
            "data_summary": {
                "logs_found": log_count,
                "traces_found": trace_count,
            },
            "analysis": analysis,
        }

    async def _extract_intent(self, query: str) -> dict:
        prompt = f"""You are an observability expert. Extract structured intent from this user query about Kubernetes services.

User query: "{query}"

Respond ONLY with valid JSON (no markdown, no explanation):
{{
  "service_name": "<service name or empty string if not specified>",
  "query_type": "<one of: anomaly_detection, slowness, errors, logs, traces, rca, general, browse>",
  "time_range_minutes": <integer, default 1440 for general queries, 60 for specific incidents, 0 means search ALL data with no time filter>,
  "severity_filter": "<ERROR, WARN, or empty string>",
  "keywords": ["<relevant search terms>"]
}}

IMPORTANT RULES:
- If the user asks to "show all", "list", "what data", "show me logs", "show me traces" or any browsing query, set query_type to "browse" and time_range_minutes to 0.
- If the user doesn't mention a specific time window, use time_range_minutes: 1440 (24 hours).
- If the user says "all data" or "everything", set time_range_minutes to 0 (no time filter).
- For service_name, use the EXACT service name as the user mentions it (e.g. "user-management-service", not "user management")."""
        raw = await self.ask_llm(prompt)
        try:
            # Try to extract JSON from response
            json_match = re.search(r'\{.*\}', raw, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse intent JSON: {raw}")
        # Fallback — use wide time range
        return {
            "service_name": "",
            "query_type": "general",
            "time_range_minutes": 1440,
            "severity_filter": "",
            "keywords": query.lower().split()[:5],
        }

    async def _query_logs(self, intent: dict) -> dict:
        must_clauses = []
        time_range = intent.get("time_range_minutes", 1440)

        # time_range_minutes=0 means no time filter (search ALL data)
        if time_range > 0:
            now = datetime.now(timezone.utc)
            from_time = (now - timedelta(minutes=time_range)).isoformat()
            must_clauses.append({"range": {"timestamp": {"gte": from_time, "lte": now.isoformat()}}})

        if intent.get("service_name"):
            must_clauses.append({"term": {"service_name": intent["service_name"]}})

        if intent.get("severity_filter"):
            must_clauses.append({"term": {"severity": intent["severity_filter"]}})

        should_clauses = []
        for kw in intent.get("keywords", []):
            should_clauses.append({"match": {"body": kw}})

        query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}
        if should_clauses:
            if "bool" not in query:
                query = {"bool": {"must": [], "should": should_clauses, "minimum_should_match": 0}}
            else:
                query["bool"]["should"] = should_clauses
                query["bool"]["minimum_should_match"] = 0

        result = await es.search(ES_LOG_INDEX, query, size=200)

        # AUTO-WIDEN: If time-filtered query returned 0 hits, retry without time filter
        hit_count = result.get("hits", {}).get("total", {}).get("value", 0)
        if hit_count == 0 and time_range > 0:
            logger.info(f"Log query returned 0 hits with {time_range}m window, retrying without time filter...")
            must_clauses_no_time = [c for c in must_clauses if "range" not in c or "timestamp" not in c.get("range", {})]
            if must_clauses_no_time:
                query_wide = {"bool": {"must": must_clauses_no_time}}
            else:
                query_wide = {"match_all": {}}
            if should_clauses:
                if "bool" not in query_wide:
                    query_wide = {"bool": {"must": [], "should": should_clauses, "minimum_should_match": 0}}
                else:
                    query_wide["bool"]["should"] = should_clauses
                    query_wide["bool"]["minimum_should_match"] = 0
            result = await es.search(ES_LOG_INDEX, query_wide, size=200)
            wider_count = result.get("hits", {}).get("total", {}).get("value", 0)
            logger.info(f"Wider log query found {wider_count} hits")

        return result

    async def _query_traces(self, intent: dict) -> dict:
        must_clauses = []
        time_range = intent.get("time_range_minutes", 1440)

        # time_range_minutes=0 means no time filter (search ALL data)
        if time_range > 0:
            now = datetime.now(timezone.utc)
            from_time = (now - timedelta(minutes=time_range)).isoformat()
            must_clauses.append({"range": {"timestamp": {"gte": from_time, "lte": now.isoformat()}}})

        if intent.get("service_name"):
            must_clauses.append({"term": {"service_name": intent["service_name"]}})

        query_type = intent.get("query_type", "general")
        if query_type in ("slowness", "anomaly_detection"):
            must_clauses.append({"range": {"duration_ms": {"gte": 500}}})
        if query_type == "errors":
            must_clauses.append({"term": {"status_code": "ERROR"}})

        query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}

        result = await es.search(ES_TRACE_INDEX, query, size=200)

        # AUTO-WIDEN: If time-filtered query returned 0 hits, retry without time filter
        hit_count = result.get("hits", {}).get("total", {}).get("value", 0)
        if hit_count == 0 and time_range > 0:
            logger.info(f"Trace query returned 0 hits with {time_range}m window, retrying without time filter...")
            must_clauses_no_time = [c for c in must_clauses if "range" not in c or "timestamp" not in c.get("range", {})]
            if must_clauses_no_time:
                query_wide = {"bool": {"must": must_clauses_no_time}}
            else:
                query_wide = {"match_all": {}}
            result = await es.search(ES_TRACE_INDEX, query_wide, size=200)
            wider_count = result.get("hits", {}).get("total", {}).get("value", 0)
            logger.info(f"Wider trace query found {wider_count} hits")

        return result

    async def _get_service_stats(self, intent: dict) -> dict:
        """Get aggregated stats: error rate, p95 latency, throughput."""
        time_range = intent.get("time_range_minutes", 1440)

        filter_clause = []
        if time_range > 0:
            now = datetime.now(timezone.utc)
            from_time = (now - timedelta(minutes=time_range)).isoformat()
            filter_clause.append({"range": {"timestamp": {"gte": from_time, "lte": now.isoformat()}}})

        if intent.get("service_name"):
            filter_clause.append({"term": {"service_name": intent["service_name"]}})

        # Build base query — match_all if no filters
        base_query = {"bool": {"filter": filter_clause}} if filter_clause else {"match_all": {}}

        # Trace stats: latency percentiles, error rate
        trace_agg_body = {
            "size": 0,
            "query": base_query,
            "aggs": {
                "latency_stats": {
                    "percentiles": {"field": "duration_ms", "percents": [50, 75, 90, 95, 99]}
                },
                "error_count": {
                    "filter": {"term": {"status_code": "ERROR"}}
                },
                "by_service": {
                    "terms": {"field": "service_name", "size": 20},
                    "aggs": {
                        "avg_duration": {"avg": {"field": "duration_ms"}},
                        "p95_duration": {"percentiles": {"field": "duration_ms", "percents": [95]}},
                        "errors": {"filter": {"term": {"status_code": "ERROR"}}},
                    },
                },
                "over_time": {
                    "date_histogram": {"field": "timestamp", "fixed_interval": "5m"},
                    "aggs": {
                        "avg_duration": {"avg": {"field": "duration_ms"}},
                        "count": {"value_count": {"field": "span_id"}},
                    },
                },
            },
        }
        trace_stats = await es.aggregate(ES_TRACE_INDEX, trace_agg_body)

        # Log stats: severity breakdown
        log_agg_body = {
            "size": 0,
            "query": base_query,
            "aggs": {
                "by_severity": {"terms": {"field": "severity", "size": 10}},
                "error_logs_over_time": {
                    "filter": {"terms": {"severity": ["ERROR", "FATAL", "CRITICAL"]}},
                    "aggs": {
                        "over_time": {
                            "date_histogram": {"field": "timestamp", "fixed_interval": "5m"},
                        }
                    },
                },
            },
        }
        log_stats = await es.aggregate(ES_LOG_INDEX, log_agg_body)

        return {"trace_stats": trace_stats, "log_stats": log_stats}

    def _extract_log_samples(self, results: dict, max_samples: int = 30) -> list[dict]:
        hits = results.get("hits", {}).get("hits", [])
        samples = []
        for h in hits[:max_samples]:
            src = h["_source"]
            samples.append({
                "timestamp": src.get("timestamp", ""),
                "service": src.get("service_name", ""),
                "severity": src.get("severity", ""),
                "body": src.get("body", "")[:500],
                "pod": src.get("k8s_pod_name", ""),
            })
        return samples

    def _extract_trace_samples(self, results: dict, max_samples: int = 30) -> list[dict]:
        hits = results.get("hits", {}).get("hits", [])
        samples = []
        for h in hits[:max_samples]:
            src = h["_source"]
            samples.append({
                "timestamp": src.get("timestamp", ""),
                "service": src.get("service_name", ""),
                "span_name": src.get("span_name", ""),
                "duration_ms": src.get("duration_ms", 0),
                "status": src.get("status_code", ""),
                "http_method": src.get("http_method", ""),
                "http_url": src.get("http_url", ""),
                "http_status": src.get("http_status_code", 0),
                "pod": src.get("k8s_pod_name", ""),
            })
        return samples

    async def _analyze_data(
        self, user_query: str, intent: dict, logs: list, traces: list, stats: dict
    ) -> str:
        # Summarize stats for the prompt
        stats_summary = self._summarize_stats(stats)

        prompt = f"""You are a senior SRE / observability expert analyzing Kubernetes application telemetry.

USER QUESTION: "{user_query}"

EXTRACTED INTENT:
- Service: {intent.get('service_name', 'all services')}
- Query type: {intent.get('query_type', 'general')}
- Time window: last {intent.get('time_range_minutes', 60)} minutes

AGGREGATED STATISTICS:
{stats_summary}

SAMPLE LOG ENTRIES ({len(logs)} shown):
{json.dumps(logs[:20], indent=2, default=str)[:4000]}

SAMPLE TRACE SPANS ({len(traces)} shown):
{json.dumps(traces[:20], indent=2, default=str)[:4000]}

Based on the above telemetry data, provide a thorough analysis. Structure your response as:

1. **Summary**: Direct answer to the user's question
2. **Key Findings**: Bullet points of notable patterns (error spikes, latency anomalies, failing endpoints)
3. **Root Cause Analysis**: If issues are found, explain the likely root cause by correlating logs and traces
4. **Affected Components**: Which services, pods, or endpoints are impacted
5. **Recommendations**: Actionable next steps to resolve or investigate further

Be specific - reference actual service names, error messages, latency values, and timestamps from the data.
If no issues are found, say so clearly.
If data is insufficient, explain what additional data would help."""

        return await self.ask_llm(prompt)

    def _summarize_stats(self, stats: dict) -> str:
        lines = []
        ts = stats.get("trace_stats", {})
        ls = stats.get("log_stats", {})

        # Trace aggregations
        aggs = ts.get("aggregations", {})
        if "latency_stats" in aggs:
            percs = aggs["latency_stats"].get("values", {})
            lines.append(f"Latency percentiles: p50={percs.get('50.0', 'N/A')}ms, p95={percs.get('95.0', 'N/A')}ms, p99={percs.get('99.0', 'N/A')}ms")

        if "error_count" in aggs:
            err_count = aggs["error_count"].get("doc_count", 0)
            total = ts.get("hits", {}).get("total", {}).get("value", 0)
            rate = (err_count / total * 100) if total > 0 else 0
            lines.append(f"Trace error rate: {err_count}/{total} ({rate:.1f}%)")

        if "by_service" in aggs:
            for bucket in aggs["by_service"].get("buckets", [])[:10]:
                svc = bucket["key"]
                avg_d = bucket.get("avg_duration", {}).get("value", 0)
                errs = bucket.get("errors", {}).get("doc_count", 0)
                lines.append(f"  Service '{svc}': avg_latency={avg_d:.1f}ms, errors={errs}, count={bucket['doc_count']}")

        # Log aggregations
        log_aggs = ls.get("aggregations", {})
        if "by_severity" in log_aggs:
            severity_counts = {b["key"]: b["doc_count"] for b in log_aggs["by_severity"].get("buckets", [])}
            lines.append(f"Log severity breakdown: {severity_counts}")

        return "\n".join(lines) if lines else "No aggregated stats available."


agent = ObservabilityAgent()


# ─── OTLP HTTP Endpoints (receive from otel-collector) ───────────────────────

@app.post("/v1/logs")
async def receive_logs(request: Request):
    """OTLP HTTP log receiver - configure otel-collector to export here."""
    payload = await request.json()
    docs = parse_otlp_logs(payload)
    await es.bulk_index(ES_LOG_INDEX, docs)
    logger.info(f"Ingested {len(docs)} log records")
    return {"status": "ok", "ingested": len(docs)}


@app.post("/v1/traces")
async def receive_traces(request: Request):
    """OTLP HTTP trace receiver - configure otel-collector to export here."""
    payload = await request.json()
    docs = parse_otlp_traces(payload)
    await es.bulk_index(ES_TRACE_INDEX, docs)
    logger.info(f"Ingested {len(docs)} trace spans")
    return {"status": "ok", "ingested": len(docs)}


# ─── Query API ────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    query: str
    time_range_minutes: Optional[int] = None


@app.post("/api/query")
async def query_agent(req: QueryRequest):
    """Natural language query endpoint."""
    result = await agent.process_query(req.query)
    return result


@app.websocket("/ws/query")
async def ws_query(websocket: WebSocket):
    """WebSocket endpoint for real-time streaming queries."""
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            user_query = msg.get("query", "")

            await websocket.send_json({"type": "status", "message": "Understanding your query..."})
            intent = await agent._extract_intent(user_query)
            await websocket.send_json({"type": "intent", "data": intent})

            await websocket.send_json({"type": "status", "message": "Searching logs & traces..."})
            log_results = await agent._query_logs(intent)
            trace_results = await agent._query_traces(intent)
            log_count = log_results.get("hits", {}).get("total", {}).get("value", 0)
            trace_count = trace_results.get("hits", {}).get("total", {}).get("value", 0)
            await websocket.send_json({
                "type": "data_summary",
                "data": {"logs_found": log_count, "traces_found": trace_count},
            })

            await websocket.send_json({"type": "status", "message": "Computing statistics..."})
            stats = await agent._get_service_stats(intent)

            log_samples = agent._extract_log_samples(log_results)
            trace_samples = agent._extract_trace_samples(trace_results)

            await websocket.send_json({"type": "status", "message": "AI is analyzing telemetry data..."})
            analysis = await agent._analyze_data(user_query, intent, log_samples, trace_samples, stats)

            await websocket.send_json({
                "type": "result",
                "data": {
                    "query": user_query,
                    "intent": intent,
                    "data_summary": {"logs_found": log_count, "traces_found": trace_count},
                    "analysis": analysis,
                },
            })
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")


# ─── Health & Status ──────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    es_health = await es.health()
    log_count = await es.index_count(ES_LOG_INDEX)
    trace_count = await es.index_count(ES_TRACE_INDEX)
    return {
        "agent": "DeepRoot AI",
        "status": "ok",
        "elasticsearch": es_health,
        "indices": {
            "logs": {"name": ES_LOG_INDEX, "doc_count": log_count},
            "traces": {"name": ES_TRACE_INDEX, "doc_count": trace_count},
        },
    }


@app.get("/api/debug")
async def debug_indices():
    """Debug endpoint — shows raw data in both indices. Use this to verify ES has your data."""
    log_result = await es.search(ES_LOG_INDEX, {"match_all": {}}, size=10)
    trace_result = await es.search(ES_TRACE_INDEX, {"match_all": {}}, size=10)

    log_count = log_result.get("hits", {}).get("total", {}).get("value", 0)
    trace_count = trace_result.get("hits", {}).get("total", {}).get("value", 0)
    log_docs = [h["_source"] for h in log_result.get("hits", {}).get("hits", [])]
    trace_docs = [h["_source"] for h in trace_result.get("hits", {}).get("hits", [])]

    return {
        "otel_logs": {"total_count": log_count, "sample_docs": log_docs},
        "otel_traces": {"total_count": trace_count, "sample_docs": trace_docs},
        "index_names": {"logs": ES_LOG_INDEX, "traces": ES_TRACE_INDEX},
    }


@app.get("/api/services")
async def list_services():
    """List all services with recent telemetry."""
    body = {
        "size": 0,
        "aggs": {
            "services": {
                "terms": {"field": "service_name", "size": 50},
                "aggs": {"latest": {"max": {"field": "timestamp"}}},
            }
        },
    }
    r1 = await es.aggregate(ES_LOG_INDEX, body)
    r2 = await es.aggregate(ES_TRACE_INDEX, body)

    services = {}
    for r, source in [(r1, "logs"), (r2, "traces")]:
        for b in r.get("aggregations", {}).get("services", {}).get("buckets", []):
            name = b["key"]
            if name not in services:
                services[name] = {"name": name, "log_count": 0, "trace_count": 0}
            if source == "logs":
                services[name]["log_count"] = b["doc_count"]
            else:
                services[name]["trace_count"] = b["doc_count"]

    return {"services": list(services.values())}


# ─── Startup ──────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    logger.info("=" * 60)
    logger.info("  DeepRoot AI - Observability Agent Starting...")
    logger.info("=" * 60)
    logger.info(f"  Elasticsearch : {ES_HOST}")
    logger.info(f"  Log Index     : {ES_LOG_INDEX}")
    logger.info(f"  Trace Index   : {ES_TRACE_INDEX}")
    logger.info(f"  Ollama        : {OLLAMA_URL}")
    logger.info(f"  Model         : {OLLAMA_MODEL}")
    logger.info("-" * 60)

    # Check ES connectivity first
    es_health = await es.health()
    if es_health.get("status") == "unreachable":
        logger.error("❌ Cannot reach Elasticsearch! Check ES_HOST configuration.")
        logger.error(f"   Tried: {ES_HOST}")
    else:
        logger.info(f"✅ Elasticsearch cluster: {es_health.get('cluster_name', 'unknown')} (status: {es_health.get('status', 'unknown')})")

    # Create indices
    await es.ensure_index(ES_LOG_INDEX, LOG_INDEX_MAPPING)
    await es.ensure_index(ES_TRACE_INDEX, TRACE_INDEX_MAPPING)

    # Verify doc counts
    log_count = await es.index_count(ES_LOG_INDEX)
    trace_count = await es.index_count(ES_TRACE_INDEX)
    logger.info(f"📊 Current data: {log_count} logs, {trace_count} traces")
    logger.info("=" * 60)
    logger.info("  DeepRoot AI ready! Open http://localhost:8080")
    logger.info("=" * 60)


# ─── UI ───────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def ui(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
