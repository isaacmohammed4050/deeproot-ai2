"""
DeepRoot AI v2.0 - AI-powered observability agent for Kubernetes workloads.

Flexible across ALL Elasticsearch indices. Reads OTLP data, plain JSON,
manually ingested docs — any data in any index. Discovers indices dynamically,
auto-detects schemas, and searches intelligently.
"""

import os
import json
import logging
import re
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
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

app = FastAPI(title="DeepRoot AI", version="2.0.0", description="AI-powered observability agent — flexible across all ES indices")

# ─── Elasticsearch Index Templates (used ONLY for OTLP pipeline ingestion) ───

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
    "settings": {"number_of_shards": 2, "number_of_replicas": 1, "index.refresh_interval": "5s"},
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
    "settings": {"number_of_shards": 2, "number_of_replicas": 1, "index.refresh_interval": "5s"},
}

# ─── Field Detection Lists (supports OTLP, ECS, plain JSON, custom schemas) ──
TIME_FIELDS = ["timestamp", "@timestamp", "time", "datetime", "date", "created_at",
               "startTime", "endTime", "timeUnixNano", "observed_timestamp"]
TEXT_FIELDS = ["body", "message", "msg", "log", "text", "content", "description",
               "error", "exception", "stacktrace", "stack_trace", "log.message"]
SERVICE_FIELDS = ["service_name", "service.name", "serviceName", "app", "application",
                  "source", "container_name", "kubernetes.container_name", "host.name",
                  "agent.name", "service"]
LEVEL_FIELDS = ["severity", "level", "log_level", "loglevel", "severity_text",
                "severityText", "priority", "log.level"]
TRACE_FIELDS = ["trace_id", "traceId", "span_id", "spanId", "span_name", "spanName",
                "duration_ms", "duration", "elapsed", "latency", "parent_span_id"]


# ─── Elasticsearch Client (with dynamic index discovery) ─────────────────────

class ESClient:
    """Async Elasticsearch client with automatic index discovery and schema detection."""

    def __init__(self, host: str):
        self.host = host.rstrip("/")
        self.client = httpx.AsyncClient(timeout=30.0)
        self.index_meta: dict[str, dict] = {}

    async def health(self) -> dict:
        try:
            r = await self.client.get(f"{self.host}/_cluster/health")
            return r.json()
        except Exception as e:
            logger.error(f"ES health check failed: {e}")
            return {"status": "unreachable", "error": str(e)}

    async def ensure_index(self, index: str, mapping: dict):
        for attempt in range(3):
            try:
                r = await self.client.head(f"{self.host}/{index}")
                if r.status_code == 404:
                    r = await self.client.put(
                        f"{self.host}/{index}", json=mapping,
                        headers={"Content-Type": "application/json"},
                    )
                    logger.info(f"Created index '{index}': {r.status_code}")
                else:
                    logger.info(f"Index '{index}' exists")
                return
            except Exception as e:
                logger.warning(f"ES attempt {attempt+1}/3 for '{index}': {e}")
                if attempt < 2:
                    await asyncio.sleep(5)

    # ── Index Discovery ──

    async def discover_indices(self) -> list[str]:
        """Find ALL user indices in the cluster."""
        try:
            r = await self.client.get(f"{self.host}/_cat/indices?format=json&h=index,docs.count,store.size")
            raw = r.json()
            return [i["index"] for i in raw if not i["index"].startswith(".")]
        except Exception as e:
            logger.error(f"Index discovery failed: {e}")
            return []

    async def get_index_fields(self, index: str) -> list[str]:
        try:
            r = await self.client.get(f"{self.host}/{index}/_mapping")
            mapping = r.json()
            fields = []
            for idx_data in mapping.values():
                props = idx_data.get("mappings", {}).get("properties", {})
                fields.extend(self._flatten_props(props))
            return fields
        except Exception:
            return []

    def _flatten_props(self, props: dict, prefix: str = "") -> list[str]:
        result = []
        for name, cfg in props.items():
            full = f"{prefix}.{name}" if prefix else name
            result.append(full)
            if "properties" in cfg:
                result.extend(self._flatten_props(cfg["properties"], full))
        return result

    async def classify_index(self, index: str) -> dict:
        """Auto-detect schema: what type of data, which fields to use for time/text/service/etc."""
        if index in self.index_meta:
            return self.index_meta[index]

        fields = await self.get_index_fields(index)
        field_set = set(fields)

        def _find(candidates):
            for c in candidates:
                if c in field_set:
                    return c
            return None

        time_field = _find(TIME_FIELDS)
        text_field = _find(TEXT_FIELDS)
        service_field = _find(SERVICE_FIELDS)
        level_field = _find(LEVEL_FIELDS)

        has_trace = any(t in field_set for t in TRACE_FIELDS)
        has_log = any(t in field_set for t in TEXT_FIELDS)
        idx_type = "traces" if has_trace else ("logs" if has_log else "unknown")

        duration_field = _find(["duration_ms", "duration", "elapsed", "latency", "response_time"])
        status_field = _find(["status_code", "status", "http_status_code", "http.status_code", "response_code"])

        meta = {
            "index": index, "type": idx_type, "fields": fields,
            "time_field": time_field, "text_field": text_field,
            "service_field": service_field, "level_field": level_field,
            "duration_field": duration_field, "status_field": status_field,
        }
        self.index_meta[index] = meta
        logger.info(f"Classified '{index}': type={idx_type}, time={time_field}, text={text_field}, svc={service_field}")
        return meta

    async def refresh_index_catalog(self):
        """Discover all indices and classify each."""
        self.index_meta.clear()
        indices = await self.discover_indices()
        for idx in indices:
            await self.classify_index(idx)
        return self.index_meta

    def get_log_indices(self) -> list[str]:
        return [n for n, m in self.index_meta.items() if m["type"] in ("logs", "unknown")]

    def get_trace_indices(self) -> list[str]:
        return [n for n, m in self.index_meta.items() if m["type"] == "traces"]

    def get_all_data_indices(self) -> list[str]:
        return list(self.index_meta.keys())

    # ── Search & Aggregation ──

    async def index_count(self, index: str) -> int:
        try:
            r = await self.client.get(f"{self.host}/{index}/_count", headers={"Content-Type": "application/json"})
            return r.json().get("count", 0)
        except Exception:
            return 0

    async def search(self, index: str, query: dict, size: int = 100) -> dict:
        try:
            meta = self.index_meta.get(index, {})
            tf = meta.get("time_field", "timestamp")
            body = {"query": query, "size": size, "sort": [{tf: {"order": "desc", "unmapped_type": "date"}}, "_score"]}

            r = await self.client.post(f"{self.host}/{index}/_search", json=body, headers={"Content-Type": "application/json"})
            if r.status_code != 200:
                # Fallback: search without sort
                body_ns = {"query": query, "size": size}
                r = await self.client.post(f"{self.host}/{index}/_search", json=body_ns, headers={"Content-Type": "application/json"})
            return r.json()
        except Exception as e:
            logger.error(f"Search failed on '{index}': {e}")
            return {"hits": {"hits": [], "total": {"value": 0}}}

    async def aggregate(self, index: str, body: dict) -> dict:
        try:
            r = await self.client.post(f"{self.host}/{index}/_search", json=body, headers={"Content-Type": "application/json"})
            return r.json()
        except Exception as e:
            logger.error(f"Aggregation failed on '{index}': {e}")
            return {}

    async def bulk_index(self, index: str, docs: list[dict]):
        if not docs:
            return
        ndjson = ""
        for doc in docs:
            ndjson += json.dumps({"index": {"_index": index}}) + "\n"
            ndjson += json.dumps(doc) + "\n"
        try:
            r = await self.client.post(f"{self.host}/_bulk", content=ndjson, headers={"Content-Type": "application/x-ndjson"})
            resp = r.json()
            if resp.get("errors"):
                errs = [i for i in resp["items"] if "error" in i.get("index", {})]
                logger.warning(f"Bulk index: {len(errs)} errors")
        except Exception as e:
            logger.error(f"Bulk index failed: {e}")


es = ESClient(ES_HOST)


# ─── OTLP Receiver ───────────────────────────────────────────────────────────

def parse_otlp_logs(payload: dict) -> list[dict]:
    docs = []
    for rl in payload.get("resourceLogs", []):
        ra = _flatten_attrs(rl.get("resource", {}).get("attributes", []))
        svc = ra.get("service.name", "unknown")
        for sl in rl.get("scopeLogs", []):
            for lr in sl.get("logRecords", []):
                docs.append({
                    "timestamp": _nano_to_iso(lr.get("timeUnixNano", "0")),
                    "service_name": svc,
                    "severity": lr.get("severityText", "UNSPECIFIED"),
                    "body": _extract_body(lr.get("body", {})),
                    "resource_attributes": ra,
                    "log_attributes": _flatten_attrs(lr.get("attributes", [])),
                    "trace_id": lr.get("traceId", ""),
                    "span_id": lr.get("spanId", ""),
                    "k8s_namespace": ra.get("k8s.namespace.name", ""),
                    "k8s_pod_name": ra.get("k8s.pod.name", ""),
                    "k8s_container_name": ra.get("k8s.container.name", ""),
                })
    return docs

def parse_otlp_traces(payload: dict) -> list[dict]:
    docs = []
    for rt in payload.get("resourceSpans", []):
        ra = _flatten_attrs(rt.get("resource", {}).get("attributes", []))
        svc = ra.get("service.name", "unknown")
        for ss in rt.get("scopeSpans", []):
            for span in ss.get("spans", []):
                s, e = int(span.get("startTimeUnixNano", "0")), int(span.get("endTimeUnixNano", "0"))
                sa = _flatten_attrs(span.get("attributes", []))
                docs.append({
                    "timestamp": _nano_to_iso(span.get("startTimeUnixNano", "0")),
                    "service_name": svc,
                    "span_name": span.get("name", ""),
                    "trace_id": span.get("traceId", ""),
                    "span_id": span.get("spanId", ""),
                    "parent_span_id": span.get("parentSpanId", ""),
                    "duration_ms": round((e - s) / 1e6, 3) if s and e else 0,
                    "status_code": span.get("status", {}).get("code", "UNSET"),
                    "status_message": span.get("status", {}).get("message", ""),
                    "span_kind": _span_kind(span.get("kind", 0)),
                    "attributes": sa,
                    "events": span.get("events", []),
                    "resource_attributes": ra,
                    "http_method": sa.get("http.method", ""),
                    "http_url": sa.get("http.url", sa.get("http.target", "")),
                    "http_status_code": int(sa.get("http.status_code", 0) or 0),
                    "k8s_namespace": ra.get("k8s.namespace.name", ""),
                    "k8s_pod_name": ra.get("k8s.pod.name", ""),
                })
    return docs

def _flatten_attrs(attrs: list) -> dict:
    r = {}
    for a in attrs:
        k, v = a.get("key", ""), a.get("value", {})
        if "stringValue" in v: r[k] = v["stringValue"]
        elif "intValue" in v: r[k] = int(v["intValue"])
        elif "doubleValue" in v: r[k] = float(v["doubleValue"])
        elif "boolValue" in v: r[k] = v["boolValue"]
        else: r[k] = str(v)
    return r

def _nano_to_iso(ns: str) -> str:
    try: return datetime.fromtimestamp(int(ns) / 1e9, tz=timezone.utc).isoformat()
    except: return datetime.now(timezone.utc).isoformat()

def _extract_body(body: dict) -> str:
    return body.get("stringValue", json.dumps(body))

def _span_kind(k: int) -> str:
    return {0:"UNSPECIFIED",1:"INTERNAL",2:"SERVER",3:"CLIENT",4:"PRODUCER",5:"CONSUMER"}.get(k, "UNKNOWN")


# ─── Smart Query Builder (schema-agnostic) ───────────────────────────────────

class SmartQueryBuilder:
    """Builds ES queries dynamically based on whatever fields the index actually has."""

    @staticmethod
    def build_search_query(intent: dict, index_meta: dict) -> dict:
        must, should = [], []

        # Time range (using whatever time field this index has)
        tf = index_meta.get("time_field")
        tr = intent.get("time_range_minutes", 0)
        if tf and tr > 0:
            now = datetime.now(timezone.utc)
            must.append({"range": {tf: {"gte": (now - timedelta(minutes=tr)).isoformat(), "lte": now.isoformat()}}})

        # Service filter (using whatever service field this index has)
        svc = intent.get("service_name", "")
        if svc:
            sf = index_meta.get("service_field")
            if sf:
                should.append({"term": {sf: svc}})
                should.append({"match": {sf: svc}})
            should.append({"multi_match": {"query": svc, "fields": ["*"], "type": "phrase"}})

        # Severity filter
        sev = intent.get("severity_filter", "")
        if sev:
            lf = index_meta.get("level_field")
            if lf:
                must.append({"term": {lf: sev}})
            else:
                should.append({"multi_match": {"query": sev, "fields": ["*"]}})

        # Keywords — search across ALL fields
        for kw in intent.get("keywords", []):
            should.append({"multi_match": {"query": kw, "fields": ["*"], "type": "best_fields", "fuzziness": "AUTO"}})

        # Query-type specific
        qt = intent.get("query_type", "general")
        if qt == "errors":
            err_should = []
            stf = index_meta.get("status_field")
            lf = index_meta.get("level_field")
            if stf:
                err_should.append({"term": {stf: "ERROR"}})
                err_should.append({"term": {stf: "error"}})
            if lf:
                err_should.append({"terms": {lf: ["ERROR", "FATAL", "CRITICAL", "error", "fatal"]}})
            err_should.append({"multi_match": {"query": "error exception fail fatal crash timeout", "fields": ["*"]}})
            must.append({"bool": {"should": err_should, "minimum_should_match": 1}})

        elif qt in ("slowness", "anomaly_detection"):
            df = index_meta.get("duration_field")
            if df:
                must.append({"range": {df: {"gte": 500}}})
            else:
                should.append({"multi_match": {"query": "slow timeout latency delay high response", "fields": ["*"]}})

        # Assemble
        if not must and not should:
            return {"match_all": {}}
        q = {"bool": {}}
        if must: q["bool"]["must"] = must
        if should:
            q["bool"]["should"] = should
            q["bool"]["minimum_should_match"] = 1 if (not must and not svc) else 0
        return q

    @staticmethod
    def build_agg_body(intent: dict, meta: dict) -> dict:
        tf = meta.get("time_field")
        sf = meta.get("service_field")
        df = meta.get("duration_field")
        lf = meta.get("level_field")
        stf = meta.get("status_field")

        aggs = {}
        if sf:
            sub = {}
            if df:
                sub["avg_duration"] = {"avg": {"field": df}}
                sub["p95_duration"] = {"percentiles": {"field": df, "percents": [95]}}
            if stf:
                sub["errors"] = {"filter": {"terms": {stf: ["ERROR", "error", "FATAL"]}}}
            agg = {"terms": {"field": sf, "size": 20}}
            if sub: agg["aggs"] = sub
            aggs["by_service"] = agg

        if df: aggs["latency_stats"] = {"percentiles": {"field": df, "percents": [50, 75, 90, 95, 99]}}
        if lf: aggs["by_level"] = {"terms": {"field": lf, "size": 10}}
        if stf: aggs["error_count"] = {"filter": {"terms": {stf: ["ERROR", "error", "FATAL", "fatal"]}}}

        if tf:
            ta = {"date_histogram": {"field": tf, "fixed_interval": "5m"}}
            if df: ta["aggs"] = {"avg_duration": {"avg": {"field": df}}}
            aggs["over_time"] = ta

        filt = []
        tr = intent.get("time_range_minutes", 0)
        if tf and tr > 0:
            now = datetime.now(timezone.utc)
            filt.append({"range": {tf: {"gte": (now - timedelta(minutes=tr)).isoformat(), "lte": now.isoformat()}}})
        if intent.get("service_name") and sf:
            filt.append({"term": {sf: intent["service_name"]}})

        base = {"bool": {"filter": filt}} if filt else {"match_all": {}}
        return {"size": 0, "query": base, "aggs": aggs} if aggs else {"size": 0, "query": base}


# ─── DeepRoot AI Agent ───────────────────────────────────────────────────────

class ObservabilityAgent:
    """Core AI agent: discovers indices, searches all data, analyzes with LLM."""

    def __init__(self):
        self.ollama_url = OLLAMA_URL
        self.model = OLLAMA_MODEL

    async def ask_llm(self, prompt: str) -> str:
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                r = await client.post(self.ollama_url, json={"model": self.model, "prompt": prompt, "stream": False})
                return r.json().get("response", "").strip()
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return f"[LLM Error] Could not reach Ollama: {e}"

    async def process_query(self, user_query: str) -> dict:
        # 1. Discover all indices
        await es.refresh_index_catalog()
        all_idx = es.get_all_data_indices()
        logger.info(f"Catalog: {len(all_idx)} indices — {all_idx}")

        # 2. Extract intent
        intent = await self._extract_intent(user_query, all_idx)
        logger.info(f"Intent: {intent}")

        # 3. Search across all relevant indices
        all_results, total_hits = await self._search_all(intent, all_idx)

        # 4. Aggregations
        stats = await self._aggregate_all(intent, [r["index"] for r in all_results] or all_idx)

        # 5. Samples for LLM
        samples = self._extract_samples(all_results, max_per_index=15)

        # 6. LLM analysis
        analysis = await self._analyze(user_query, intent, samples, stats, all_results)

        return {
            "query": user_query,
            "intent": intent,
            "data_summary": {
                "total_hits": total_hits,
                "indices_searched": [r["index"] for r in all_results],
                "breakdown": {r["index"]: r["hits"] for r in all_results},
            },
            "analysis": analysis,
        }

    async def _search_all(self, intent: dict, all_idx: list[str]) -> tuple[list[dict], int]:
        """Search across all relevant indices with auto-widen fallback."""
        all_results = []
        total_hits = 0

        # Choose target indices
        qt = intent.get("query_type", "general")
        target_idx = intent.get("target_indices") or []
        # Filter to only existing indices
        target_idx = [i for i in target_idx if i in all_idx]
        if not target_idx:
            trace_idx = es.get_trace_indices()
            log_idx = es.get_log_indices()
            if qt == "traces" and trace_idx:
                target_idx = trace_idx
            elif qt == "logs" and log_idx:
                target_idx = log_idx
            else:
                target_idx = all_idx

        for idx in target_idx:
            meta = es.index_meta.get(idx, {})
            query = SmartQueryBuilder.build_search_query(intent, meta)
            result = await es.search(idx, query, size=100)
            hits = result.get("hits", {}).get("total", {}).get("value", 0)

            # Auto-widen: retry without time filter if 0 hits
            if hits == 0 and intent.get("time_range_minutes", 0) > 0:
                wide_intent = {**intent, "time_range_minutes": 0}
                query_wide = SmartQueryBuilder.build_search_query(wide_intent, meta)
                result = await es.search(idx, query_wide, size=100)
                hits = result.get("hits", {}).get("total", {}).get("value", 0)

            # Final fallback: match_all
            if hits == 0:
                result = await es.search(idx, {"match_all": {}}, size=50)
                hits = result.get("hits", {}).get("total", {}).get("value", 0)

            if hits > 0:
                all_results.append({"index": idx, "type": meta.get("type", "unknown"), "result": result, "hits": hits})
                total_hits += hits

        logger.info(f"Search: {total_hits} hits across {[r['index'] for r in all_results]}")
        return all_results, total_hits

    async def _aggregate_all(self, intent: dict, indices: list[str]) -> dict:
        stats = {}
        for idx in indices:
            meta = es.index_meta.get(idx, {})
            body = SmartQueryBuilder.build_agg_body(intent, meta)
            if body.get("aggs"):
                result = await es.aggregate(idx, body)
                aggs = result.get("aggregations", {})
                if aggs:
                    stats[idx] = aggs
        return stats

    async def _extract_intent(self, query: str, indices: list[str]) -> dict:
        idx_str = ", ".join(indices) if indices else "none found"
        prompt = f"""You are an observability expert. Extract structured intent from this user query.

AVAILABLE ELASTICSEARCH INDICES: {idx_str}

User query: "{query}"

Respond ONLY with valid JSON (no markdown):
{{
  "service_name": "<service name or empty string>",
  "query_type": "<anomaly_detection|slowness|errors|logs|traces|rca|general|browse>",
  "time_range_minutes": <0=ALL data, 60=last hour, 1440=last 24h>,
  "severity_filter": "<ERROR|WARN|empty>",
  "keywords": ["<search terms>"],
  "target_indices": ["<specific index names from list, or empty to search all>"]
}}

RULES:
- Default time_range_minutes to 0 (search ALL data) unless user specifies a time.
- If user mentions an index name, include it in target_indices.
- keywords should include service names, error messages, endpoints mentioned.
- browse query_type means show all available data."""
        raw = await self.ask_llm(prompt)
        try:
            m = re.search(r'\{.*\}', raw, re.DOTALL)
            if m:
                return json.loads(m.group())
        except json.JSONDecodeError:
            pass
        return {"service_name": "", "query_type": "general", "time_range_minutes": 0,
                "severity_filter": "", "keywords": query.lower().split()[:5], "target_indices": []}

    def _extract_samples(self, all_results: list[dict], max_per_index: int = 15) -> list[dict]:
        samples = []
        for r in all_results:
            for h in r["result"].get("hits", {}).get("hits", [])[:max_per_index]:
                sample = {"_index": r["index"], "_type": r["type"]}
                sample.update(h["_source"])
                samples.append(sample)
        return samples

    def _summarize_stats(self, stats: dict) -> str:
        lines = []
        for idx, aggs in stats.items():
            lines.append(f"\n--- Index: {idx} ---")
            if "latency_stats" in aggs:
                p = aggs["latency_stats"].get("values", {})
                lines.append(f"  Latency: p50={p.get('50.0','N/A')}ms p95={p.get('95.0','N/A')}ms p99={p.get('99.0','N/A')}ms")
            if "error_count" in aggs:
                lines.append(f"  Errors: {aggs['error_count'].get('doc_count', 0)}")
            if "by_service" in aggs:
                for b in aggs["by_service"].get("buckets", [])[:10]:
                    parts = [f"count={b['doc_count']}"]
                    ad = b.get("avg_duration", {}).get("value")
                    if ad is not None: parts.append(f"avg_latency={ad:.1f}ms")
                    er = b.get("errors", {}).get("doc_count", 0)
                    if er: parts.append(f"errors={er}")
                    lines.append(f"  Service '{b['key']}': {', '.join(parts)}")
            if "by_level" in aggs:
                lines.append(f"  Severity: { {b['key']:b['doc_count'] for b in aggs['by_level'].get('buckets',[])} }")
        return "\n".join(lines) if lines else "No aggregation stats available."

    async def _analyze(self, query: str, intent: dict, samples: list, stats: dict, results: list) -> str:
        stats_text = self._summarize_stats(stats)
        idx_info = ", ".join([f"{r['index']}({r['hits']})" for r in results]) or "no data found"
        samples_json = json.dumps(samples[:25], indent=2, default=str)
        if len(samples_json) > 6000:
            samples_json = samples_json[:6000] + "\n...(truncated)"

        prompt = f"""You are DeepRoot AI, a senior SRE analyzing Kubernetes telemetry from Elasticsearch.

USER QUESTION: "{query}"

INTENT: service={intent.get('service_name') or 'all'}, type={intent.get('query_type')}, time={'all data' if intent.get('time_range_minutes',0)==0 else f"{intent['time_range_minutes']}min"}

INDICES WITH DATA: {idx_info}

AGGREGATED STATS:
{stats_text}

SAMPLE DOCUMENTS:
{samples_json}

Provide thorough analysis:
1. **Summary**: Direct answer to the question
2. **Key Findings**: Patterns, error spikes, latency anomalies, notable log entries
3. **Root Cause Analysis**: If issues exist, correlate logs and traces to find the cause
4. **Affected Components**: Services, pods, endpoints impacted
5. **Recommendations**: Actionable next steps

IMPORTANT:
- Data could be OTLP format OR plain JSON — analyze whatever structure you see.
- Reference actual values: service names, error messages, latency numbers, timestamps.
- If no issues found, confirm services are healthy.
- If no data found, explain what to check (index names, data ingestion, mappings)."""
        return await self.ask_llm(prompt)


agent = ObservabilityAgent()


# ─── OTLP Endpoints ──────────────────────────────────────────────────────────

@app.post("/v1/logs")
async def receive_logs(request: Request):
    payload = await request.json()
    docs = parse_otlp_logs(payload)
    await es.bulk_index(ES_LOG_INDEX, docs)
    logger.info(f"Ingested {len(docs)} logs → '{ES_LOG_INDEX}'")
    return {"status": "ok", "ingested": len(docs), "index": ES_LOG_INDEX}

@app.post("/v1/traces")
async def receive_traces(request: Request):
    payload = await request.json()
    docs = parse_otlp_traces(payload)
    await es.bulk_index(ES_TRACE_INDEX, docs)
    logger.info(f"Ingested {len(docs)} traces → '{ES_TRACE_INDEX}'")
    return {"status": "ok", "ingested": len(docs), "index": ES_TRACE_INDEX}


# ─── Query API ────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    query: str
    time_range_minutes: Optional[int] = None

@app.post("/api/query")
async def query_agent(req: QueryRequest):
    return await agent.process_query(req.query)

@app.websocket("/ws/query")
async def ws_query(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            user_query = msg.get("query", "")

            await websocket.send_json({"type": "status", "message": "Discovering all Elasticsearch indices..."})
            await es.refresh_index_catalog()
            all_idx = es.get_all_data_indices()
            await websocket.send_json({"type": "status", "message": f"Found {len(all_idx)} indices: {', '.join(all_idx[:8])}"})

            await websocket.send_json({"type": "status", "message": "Understanding your query..."})
            intent = await agent._extract_intent(user_query, all_idx)
            await websocket.send_json({"type": "intent", "data": intent})

            await websocket.send_json({"type": "status", "message": "Searching across all indices..."})
            all_results, total_hits = await agent._search_all(intent, all_idx)

            await websocket.send_json({
                "type": "data_summary",
                "data": {"total_hits": total_hits, "indices": {r["index"]: r["hits"] for r in all_results}},
            })

            await websocket.send_json({"type": "status", "message": "Computing statistics..."})
            stats = await agent._aggregate_all(intent, [r["index"] for r in all_results] or all_idx)
            samples = agent._extract_samples(all_results)

            await websocket.send_json({"type": "status", "message": "DeepRoot AI is analyzing..."})
            analysis = await agent._analyze(user_query, intent, samples, stats, all_results)

            await websocket.send_json({
                "type": "result",
                "data": {
                    "query": user_query, "intent": intent,
                    "data_summary": {"total_hits": total_hits, "indices_searched": [r["index"] for r in all_results],
                                     "breakdown": {r["index"]: r["hits"] for r in all_results}},
                    "analysis": analysis,
                },
            })
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")


# ─── Health & Debug ───────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    h = await es.health()
    await es.refresh_index_catalog()
    idx_info = {}
    for idx in es.get_all_data_indices():
        c = await es.index_count(idx)
        m = es.index_meta.get(idx, {})
        idx_info[idx] = {"docs": c, "type": m.get("type"), "time_field": m.get("time_field"), "text_field": m.get("text_field")}
    return {"agent": "DeepRoot AI", "version": "2.0.0", "status": "ok", "elasticsearch": h, "indices": idx_info}

@app.get("/api/debug")
async def debug_indices():
    """Shows ALL discovered indices, detected schemas, and sample docs from each."""
    await es.refresh_index_catalog()
    info = {}
    for idx, meta in es.index_meta.items():
        r = await es.search(idx, {"match_all": {}}, size=5)
        info[idx] = {
            "total_docs": r.get("hits", {}).get("total", {}).get("value", 0),
            "detected_type": meta.get("type"),
            "schema": {k: meta.get(k) for k in ["time_field", "text_field", "service_field", "level_field", "duration_field", "status_field"]},
            "all_fields": meta.get("fields", []),
            "sample_docs": [h["_source"] for h in r.get("hits", {}).get("hits", [])],
        }
    return {"indices": info}

@app.get("/api/indices")
async def list_indices():
    await es.refresh_index_catalog()
    result = []
    for idx, meta in es.index_meta.items():
        c = await es.index_count(idx)
        result.append({"name": idx, "type": meta.get("type"), "doc_count": c,
                        "time_field": meta.get("time_field"), "service_field": meta.get("service_field")})
    return {"indices": result}

@app.get("/api/services")
async def list_services():
    await es.refresh_index_catalog()
    services = {}
    for idx, meta in es.index_meta.items():
        sf = meta.get("service_field")
        if not sf: continue
        r = await es.aggregate(idx, {"size": 0, "aggs": {"svcs": {"terms": {"field": sf, "size": 50}}}})
        for b in r.get("aggregations", {}).get("svcs", {}).get("buckets", []):
            n = b["key"]
            if n not in services: services[n] = {"name": n, "indices": [], "total_docs": 0}
            services[n]["indices"].append(idx)
            services[n]["total_docs"] += b["doc_count"]
    return {"services": list(services.values())}


# ─── Startup ──────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    logger.info("=" * 60)
    logger.info("  DeepRoot AI v2.0 — Flexible Observability Agent")
    logger.info("=" * 60)
    logger.info(f"  Elasticsearch : {ES_HOST}")
    logger.info(f"  OTLP Logs →   : {ES_LOG_INDEX}")
    logger.info(f"  OTLP Traces → : {ES_TRACE_INDEX}")
    logger.info(f"  Ollama        : {OLLAMA_URL} ({OLLAMA_MODEL})")
    logger.info("-" * 60)

    h = await es.health()
    if h.get("status") == "unreachable":
        logger.error("Cannot reach Elasticsearch!")
    else:
        logger.info(f"ES cluster: {h.get('cluster_name','?')} ({h.get('status','?')})")

    await es.ensure_index(ES_LOG_INDEX, LOG_INDEX_MAPPING)
    await es.ensure_index(ES_TRACE_INDEX, TRACE_INDEX_MAPPING)

    await es.refresh_index_catalog()
    for idx in es.get_all_data_indices():
        c = await es.index_count(idx)
        m = es.index_meta.get(idx, {})
        logger.info(f"  {idx}: {c} docs | type={m.get('type')} time={m.get('time_field')} text={m.get('text_field')} svc={m.get('service_field')}")

    logger.info("=" * 60)
    logger.info("  DeepRoot AI ready — searching ALL indices")
    logger.info("=" * 60)

@app.get("/", response_class=HTMLResponse)
async def ui(request: Request):
    return HTMLResponse(content=UI_HTML)


# ─── Embedded UI HTML (no external files needed) ─────────────────────────────

UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
    <title>DeepRoot AI - Observability Agent</title>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root{--bg0:#0a0e17;--bg1:#111827;--bg2:#1a2234;--bg3:#0f1623;--bd:#1e2d45;--bdf:#3b82f6;--t1:#e2e8f0;--t2:#8899b4;--t3:#4a5e7a;--ac:#3b82f6;--acg:rgba(59,130,246,.15);--gn:#10b981;--rd:#ef4444;--cy:#06b6d4}
        *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
        body{font-family:'DM Sans',sans-serif;background:var(--bg0);color:var(--t1);min-height:100vh;display:flex;flex-direction:column}
        header{padding:16px 24px;border-bottom:1px solid var(--bd);display:flex;align-items:center;justify-content:space-between;background:var(--bg1)}
        .logo{display:flex;align-items:center;gap:12px}
        .logo-icon{width:36px;height:36px;border-radius:10px;background:linear-gradient(135deg,var(--ac),var(--cy));display:flex;align-items:center;justify-content:center;font-size:18px}
        .logo h1{font-size:16px;font-weight:600}
        .logo span{font-size:12px;color:var(--t3);font-family:'JetBrains Mono',monospace}
        .status-badge{display:flex;align-items:center;gap:6px;font-size:12px;color:var(--t2);padding:6px 12px;border-radius:20px;border:1px solid var(--bd);background:var(--bg2)}
        .status-dot{width:7px;height:7px;border-radius:50%;background:var(--gn);animation:pulse 2s infinite}
        @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
        .container{flex:1;display:flex;flex-direction:column;max-width:960px;width:100%;margin:0 auto;padding:24px}
        .chat-area{flex:1;overflow-y:auto;display:flex;flex-direction:column;gap:20px;padding-bottom:24px}
        .welcome{text-align:center;padding:60px 20px}
        .welcome h2{font-size:22px;font-weight:600;margin-bottom:8px}
        .welcome p{color:var(--t2);font-size:14px;line-height:1.6;max-width:500px;margin:0 auto 24px}
        .suggestions{display:flex;flex-wrap:wrap;gap:8px;justify-content:center}
        .suggestion{padding:8px 16px;border:1px solid var(--bd);border-radius:20px;font-size:13px;color:var(--t2);cursor:pointer;background:var(--bg2);transition:all .2s;font-family:'DM Sans',sans-serif}
        .suggestion:hover{border-color:var(--ac);color:var(--ac);background:var(--acg)}
        .message{display:flex;gap:12px;animation:fadeUp .3s ease}
        @keyframes fadeUp{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
        .message.user{justify-content:flex-end}
        .message.user .msg-content{background:var(--ac);color:#fff;border-radius:16px 16px 4px 16px;max-width:70%}
        .msg-avatar{width:32px;height:32px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:14px;flex-shrink:0;background:var(--bg2);border:1px solid var(--bd)}
        .message.user .msg-avatar{display:none}
        .msg-content{padding:12px 16px;border-radius:4px 16px 16px 16px;background:var(--bg2);border:1px solid var(--bd);font-size:14px;line-height:1.65;max-width:85%}
        .msg-content p{margin-bottom:8px}.msg-content p:last-child{margin-bottom:0}
        .msg-content strong{color:var(--ac);font-weight:600}
        .msg-content ul,.msg-content ol{padding-left:18px;margin:6px 0}
        .msg-content li{margin:4px 0}
        .msg-content code{font-family:'JetBrains Mono',monospace;background:rgba(255,255,255,.06);padding:2px 6px;border-radius:4px;font-size:12px}
        .status-msg{display:flex;align-items:center;gap:8px;padding:8px 16px;font-size:12px;color:var(--t3);font-family:'JetBrains Mono',monospace;animation:fadeUp .2s ease}
        .status-msg .spinner{width:14px;height:14px;border:2px solid var(--bd);border-top-color:var(--ac);border-radius:50%;animation:spin .8s linear infinite}
        @keyframes spin{to{transform:rotate(360deg)}}
        .data-summary{display:flex;gap:12px;padding:4px 0;animation:fadeUp .2s ease;flex-wrap:wrap}
        .data-card{padding:10px 16px;border-radius:10px;border:1px solid var(--bd);background:var(--bg2);font-family:'JetBrains Mono',monospace;font-size:12px}
        .data-card .label{color:var(--t3);margin-bottom:2px}.data-card .value{font-size:20px;font-weight:600;color:var(--cy)}
        .input-area{padding-top:16px;border-top:1px solid var(--bd)}
        .input-row{display:flex;gap:8px;align-items:flex-end}
        .input-row textarea{flex:1;padding:14px 16px;border:1px solid var(--bd);border-radius:14px;background:var(--bg3);color:var(--t1);font-family:'DM Sans',sans-serif;font-size:14px;resize:none;outline:none;min-height:48px;max-height:120px;transition:border-color .2s;line-height:1.5}
        .input-row textarea:focus{border-color:var(--ac);box-shadow:0 0 0 3px var(--acg)}
        .input-row textarea::placeholder{color:var(--t3)}
        .send-btn{width:48px;height:48px;border-radius:14px;border:none;background:var(--ac);color:#fff;cursor:pointer;display:flex;align-items:center;justify-content:center;transition:all .2s;flex-shrink:0}
        .send-btn:hover{background:#2563eb;transform:scale(1.04)}.send-btn:disabled{opacity:.4;cursor:not-allowed;transform:none}
        .send-btn svg{width:20px;height:20px}
    </style>
</head>
<body>
    <header>
        <div class="logo"><div class="logo-icon">&#x2B22;</div><div><h1>DeepRoot AI</h1><span>observability x intelligence</span></div></div>
        <div class="status-badge" id="statusBadge"><div class="status-dot"></div><span>Connecting...</span></div>
    </header>
    <div class="container">
        <div class="chat-area" id="chatArea">
            <div class="welcome" id="welcome">
                <h2>DeepRoot AI &mdash; What's happening in your cluster?</h2>
                <p>Ask about your services in plain English. I'll search logs and traces across ALL Elasticsearch indices, detect anomalies, and run root cause analysis powered by AI.</p>
                <div class="suggestions">
                    <button class="suggestion" onclick="useSuggestion(this)">Show me all data in elasticsearch</button>
                    <button class="suggestion" onclick="useSuggestion(this)">Is there any spike in user-management-service?</button>
                    <button class="suggestion" onclick="useSuggestion(this)">Show me errors across all indices</button>
                    <button class="suggestion" onclick="useSuggestion(this)">Which service has the highest latency?</button>
                </div>
            </div>
        </div>
        <div class="input-area">
            <div class="input-row">
                <textarea id="queryInput" placeholder="Ask about your services... e.g. 'Any errors in order-service?'" rows="1" onkeydown="handleKey(event)"></textarea>
                <button class="send-btn" id="sendBtn" onclick="sendQuery()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12h14M12 5l7 7-7 7"/></svg></button>
            </div>
        </div>
    </div>
<script>
let ws, isProcessing=false;
function connectWS(){
    const p=location.protocol==='https:'?'wss:':'ws:';
    ws=new WebSocket(p+'//'+location.host+'/ws/query');
    ws.onopen=()=>{document.querySelector('#statusBadge span').textContent='Connected';document.querySelector('.status-dot').style.background='var(--gn)'};
    ws.onclose=()=>{document.querySelector('#statusBadge span').textContent='Disconnected';document.querySelector('.status-dot').style.background='var(--rd)';setTimeout(connectWS,3000)};
    ws.onmessage=(e)=>handleWSMessage(JSON.parse(e.data));
}
function handleWSMessage(msg){
    const chat=document.getElementById('chatArea');
    if(msg.type==='status'){
        const prev=chat.querySelector('.status-msg:last-of-type');if(prev)prev.remove();
        const el=document.createElement('div');el.className='status-msg';
        el.innerHTML='<div class="spinner"></div><span>'+msg.message+'</span>';chat.appendChild(el);
    } else if(msg.type==='data_summary'){
        const el=document.createElement('div');el.className='data-summary';let cards='';
        if(msg.data.total_hits!==undefined){
            cards+='<div class="data-card"><div class="label">Total Hits</div><div class="value">'+msg.data.total_hits.toLocaleString()+'</div></div>';
            if(msg.data.indices){for(const[idx,count]of Object.entries(msg.data.indices)){cards+='<div class="data-card"><div class="label">'+idx+'</div><div class="value">'+count.toLocaleString()+'</div></div>';}}
        }
        el.innerHTML=cards;chat.appendChild(el);
    } else if(msg.type==='result'){
        chat.querySelectorAll('.status-msg').forEach(e=>e.remove());
        addMessage('agent',msg.data.analysis);isProcessing=false;
        document.getElementById('sendBtn').disabled=false;document.getElementById('queryInput').focus();
    }
    chat.scrollTop=chat.scrollHeight;
}
function addMessage(role,text){
    const chat=document.getElementById('chatArea'),w=document.getElementById('welcome');if(w)w.style.display='none';
    const div=document.createElement('div');div.className='message '+role;
    let html=text.replace(/\\*\\*(.*?)\\*\\*/g,'<strong>$1</strong>').replace(/`(.*?)`/g,'<code>$1</code>')
        .replace(/^### (.+)$/gm,'<p><strong>$1</strong></p>').replace(/^## (.+)$/gm,'<p><strong>$1</strong></p>')
        .replace(/^- (.+)$/gm,'&bull; $1<br>').replace(/\\n\\n/g,'</p><p>').replace(/\\n/g,'<br>');
    html='<p>'+html+'</p>';
    div.innerHTML=role==='agent'?'<div class="msg-avatar">&#x2B22;</div><div class="msg-content">'+html+'</div>':'<div class="msg-content">'+text+'</div>';
    chat.appendChild(div);chat.scrollTop=chat.scrollHeight;
}
function sendQuery(){
    const input=document.getElementById('queryInput'),q=input.value.trim();if(!q||isProcessing)return;
    isProcessing=true;document.getElementById('sendBtn').disabled=true;addMessage('user',q);input.value='';input.style.height='auto';
    if(ws&&ws.readyState===WebSocket.OPEN){ws.send(JSON.stringify({query:q}))}
    else{fetch('/api/query',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({query:q})})
        .then(r=>r.json()).then(d=>{addMessage('agent',d.analysis);isProcessing=false;document.getElementById('sendBtn').disabled=false})
        .catch(e=>{addMessage('agent','Error: '+e.message);isProcessing=false;document.getElementById('sendBtn').disabled=false})}
}
function handleKey(e){if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendQuery()}const t=e.target;setTimeout(()=>{t.style.height='auto';t.style.height=t.scrollHeight+'px'},0)}
function useSuggestion(btn){document.getElementById('queryInput').value=btn.textContent;sendQuery()}
connectWS();
</script>
</body>
</html>"""
