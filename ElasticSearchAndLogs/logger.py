import os
import re
import sys
import time
import socket
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

if sys.version_info < (3, 7):
    
    sys.exit("Python 3.7+ required")

try:
    from elasticsearch import Elasticsearch
    import elasticsearch as _es_module
    _v = _es_module.__version__
    ES_VERSION = int(_v[0] if isinstance(_v, tuple) else _v.split(".")[0])

    # ElasticsearchException moved between versions — import defensively
    try:
        from elasticsearch import ElasticsearchException
    except ImportError:
        try:
            from elasticsearch.exceptions import ElasticsearchException
        except ImportError:
            ElasticsearchException = Exception  # safe fallback

except ImportError:
    sys.exit("elasticsearch-py not installed.\n  pip install elasticsearch python-dotenv")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional; fall back to real env vars


# ── logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("elastic-shipper")


# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_server_ip() -> str:
    override = os.getenv("SERVER_IP", "").strip()
    if override:
        return override
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        return "unknown"


def utc_now_iso() -> str:
    """Return current UTC time as an ES-compatible ISO-8601 string with ms precision."""
    now = datetime.now(timezone.utc)
    ms = now.microsecond // 1000
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


# ─────────────────────────────────────────────────────────────────────────────
#  Log line parser
# ─────────────────────────────────────────────────────────────────────────────

_TS  = r"(?P<ts>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,Z]\d+)?Z?)"
_LVL = r"(?P<level>[A-Z]{3,10})"

LOG_PATTERNS: List[re.Pattern] = [
    # FORMAT 1 — ETL script
    # 2026-02-04 11:38:03,655 - __CUSTOMER__ - INFO - Starting ETL Script...
    re.compile(rf"^{_TS}\s+-\s+\S+\s+-\s+{_LVL}\s+-\s+(?P<message>.+)$"),

    # FORMAT 2 — Odoo (timestamp + PID + level + DB + logger: message)
    # 2026-04-09 11:48:28,734 24 INFO new_db odoo.addons.base.models.ir_cron: Starting job...
    re.compile(rf"^{_TS}\s+\d+\s+{_LVL}\s+\S+\s+(?P<logger>[^\s:]+):\s*(?P<message>.+)$"),

    # FORMAT 3 — Go / risk-processor (tab-separated, optional trailing JSON)
    # 2026-04-07T21:50:34.647Z\tINFO\tapplication/mv_refresh_monitor.go:152\tmessage\t{...}
    re.compile(rf"^{_TS}\t{_LVL}\t(?P<logger>\S+)\t(?P<message>[^\t]+)(?:\t.*)?$"),

    # FORMAT 4 — ETL scheduler (timestamp: message, no level)
    # 2026-04-03 23:02:26: Starting ETL cycle
    re.compile(rf"^{_TS}:\s+(?P<message>.+)$"),

    # FALLBACK — generic: timestamp level message (with optional PID)
    re.compile(rf"^{_TS}\s+(?:\d+\s+)?{_LVL}\s+(?P<message>.+)$"),
]

VALID_LEVELS = frozenset({
    "DEBUG", "INFO", "WARNING", "WARN", "ERROR",
    "CRITICAL", "FATAL", "NOTICE", "TRACE", "SEVERE",
})



def parse_line(raw: str) -> Optional[Dict[str, Any]]:
    line = raw.strip()
    if not line:
        return None

    for pat in LOG_PATTERNS:
        m = pat.match(line)
        if m:
            gd = m.groupdict()
            level = gd.get("level") or "INFO"   # Format 4 has no level, default INFO
            level = level.upper()
            return {
                "@timestamp": utc_now_iso(),
                "timestamp":  gd.get("ts", ""),
                "level":      level if level in VALID_LEVELS else "UNKNOWN",
                "logger":     gd.get("logger", ""),
                "message":    gd.get("message", line).strip(),
                "raw":        line,
            }

    # fallback
    return {
        "@timestamp": utc_now_iso(),
        "timestamp":  "",
        "level":      "UNKNOWN",
        "logger":     "",
        "message":    line,
        "raw":        line,
    }

# ─────────────────────────────────────────────────────────────────────────────
#  Elasticsearch helpers  (v7 / v8 compatible)
# ─────────────────────────────────────────────────────────────────────────────

def es_create_index(client: Elasticsearch, index: str) -> None:
    """Create index with typed mapping — works on both v7 and v8."""
    mappings = {
        "properties": {
            "@timestamp": {"type": "date"},
            "timestamp":  {"type": "keyword"},
            "level":      {"type": "keyword"},
            "logger":     {"type": "keyword"},
            "log_type":   {"type": "keyword"},
            "source":     {"type": "keyword"},
            "server_ip":  {"type": "keyword"},
            "message":    {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
            },
            "raw": {"type": "text"},
        }
    }
    settings = {"number_of_shards": 1, "number_of_replicas": 1}

    if ES_VERSION >= 8:
        client.indices.create(index=index, mappings=mappings, settings=settings)
    else:
        # v7 still accepts body=
        client.indices.create(
            index=index,
            body={"mappings": mappings, "settings": settings},
        )


def es_bulk(client: Elasticsearch, operations: List[Any], timeout: str = "30s") -> Dict:
    """Bulk index — uses the correct parameter name for v7 vs v8."""
    if ES_VERSION >= 8:
        return client.bulk(operations=operations, timeout=timeout)
    else:
        return client.bulk(body=operations, timeout=timeout)


def es_index_one(client: Elasticsearch, index: str, doc: Dict[str, Any]) -> Dict:
    """Single-document index — works on both versions."""
    if ES_VERSION >= 8:
        return client.index(index=index, document=doc)
    else:
        return client.index(index=index, body=doc)


# ─────────────────────────────────────────────────────────────────────────────
#  Main shipper class
# ─────────────────────────────────────────────────────────────────────────────

class ElasticSearchLogger:
    # Tunables
    POLL_INTERVAL:    float = 1.0    # seconds between scan cycles
    BULK_SIZE:        int   = 50     # docs buffered before a bulk flush
    MAX_FILE_SIZE_MB: int   = 30     # truncation threshold
    KEEP_LAST_LINES:  int   = 1_000  # lines to keep after truncation

    def __init__(self) -> None:
        self.index      = os.getenv("ELASTICSEARCH_INDEX", "icomply-sterling-logs")
        self.server_ip  = get_server_ip()
        self.sources    = self._build_sources()
        self.es         = self._connect()
        self._buf: List[Dict[str, Any]] = []

    # ── initialisation ────────────────────────────────────────────────────────

    def _build_sources(self) -> List[Dict[str, Any]]:
        candidates = [
            (
                os.getenv("APP_LOG_PATH") or os.getenv("LOG_PATH", "/var/log/icomply/ServerLog.log"),
                "app_log", "ServerLog.log",
            ),
            (os.getenv("ETL_SCHEDULER_LOG_PATH"), "etl_scheduler", "cron_execution.log"),
            (os.getenv("ETL_ENGINE_LOG_PATH"),     "etl_engine",    "ETL.log"),
            (os.getenv("ETL_UPDATE_LOG_PATH"),     "etl_update",    "UpdateScript.log"),
            (os.getenv("RISK_PROCESSOR_LOG_PATH"), "risk_processor","risk-processor.log"),
        ]
        sources = []
        for path, log_type, name in candidates:
            if path:
                sources.append({
                    "path":     path,
                    "log_type": log_type,
                    "source":   name,
                    "pos":      0,       # byte offset
                    "inode":    None,    # for rotation detection
                })
        return sources

    def _connect(self) -> Optional[Elasticsearch]:
        uri        = os.getenv("ELASTICSEARCH_URI", "http://172.20.110.75:9200")
        username   = os.getenv("ELASTICSEARCH_USERNAME", "")
        password   = os.getenv("ELASTICSEARCH_PASSWORD", "")
        ignore_ssl = os.getenv("ELASTICSEARCH_IGNORE_SSL_ERRORS", "true").lower() == "true"

        opts: Dict[str, Any] = {
            "hosts":            [uri],
            "request_timeout":  30,
            "retry_on_timeout": True,
            "max_retries":      3,
        }
        if username and password:
            opts["basic_auth" if ES_VERSION >= 8 else "http_auth"] = (username, password)
        if uri.startswith("https://"):
            opts["verify_certs"] = not ignore_ssl

        try:
            client = Elasticsearch(**opts)
            info   = client.info()
            log.info(
                "elasticsearch-py v%d | cluster '%s' (ES %s) | index '%s'",
                ES_VERSION,
                info["cluster_name"],
                info["version"]["number"],
                self.index,
            )
            if not client.indices.exists(index=self.index):
                es_create_index(client, self.index)
                log.info("Created index '%s'", self.index)
            else:
                log.info("Index '%s' already exists", self.index)
            return client
        except Exception as exc:
            log.error("Could not connect to Elasticsearch: %s", exc)
            return None

    # ── buffered shipping ─────────────────────────────────────────────────────

    def _enrich(self, entry: Dict[str, Any], src: Dict[str, Any]) -> Dict[str, Any]:
        entry.update(
            log_type=src["log_type"],
            source=src["source"],
            server_ip=self.server_ip,
        )
        return entry

    def _flush(self) -> None:
        if not self._buf or not self.es:
            return

        docs        = self._buf[:]   # snapshot
        self._buf.clear()            # clear before any I/O so errors don't double-ship

        operations: List[Any] = []
        for doc in docs:
            operations.append({"index": {"_index": self.index}})
            operations.append(doc)

        try:
            resp = es_bulk(self.es, operations)
            errors = [
                item["index"]["error"]
                for item in resp.get("items", [])
                if item.get("index", {}).get("error")
            ]
            if errors:
                for err in errors:
                    log.warning("Bulk item error: %s", err)
            log.info("Flushed %d docs → '%s'", len(docs), self.index)
        except ElasticsearchException as exc:
            log.error("Bulk failed (%s) — falling back to one-by-one", exc)
            self._ship_one_by_one(docs)

    def _ship_one_by_one(self, docs: List[Dict[str, Any]]) -> None:
        ok = 0
        for doc in docs:
            try:
                resp = es_index_one(self.es, self.index, doc)
                if resp.get("result") in ("created", "updated"):
                    ok += 1
                else:
                    log.warning("Unexpected result: %s", resp.get("result"))
            except Exception as exc:
                log.error("Failed to index doc: %s", exc)
        log.info("One-by-one: %d/%d docs shipped", ok, len(docs))

    def _buffer(self, doc: Dict[str, Any]) -> None:
        self._buf.append(doc)
        if len(self._buf) >= self.BULK_SIZE:
            self._flush()

    # ── file helpers ──────────────────────────────────────────────────────────

    def _detect_rotation(self, src: Dict[str, Any]) -> None:
        try:
            inode = os.stat(src["path"]).st_ino
            if src["inode"] is None:
                src["inode"] = inode
            elif src["inode"] != inode:
                log.info("Rotation detected: %s", src["path"])
                src["inode"] = inode
                src["pos"]   = 0
        except OSError:
            pass

    def _read_new(self, src: Dict[str, Any]) -> List[str]:
        try:
            size = os.path.getsize(src["path"])
        except OSError:
            return []

        if size < src["pos"]:
            log.info("File shrank, resetting: %s", src["path"])
            src["pos"] = 0

        if size == src["pos"]:
            return []

        try:
            with open(src["path"], "r", errors="replace") as fh:
                fh.seek(src["pos"])
                lines = fh.readlines()
            src["pos"] = size
        except OSError as exc:
            log.error("Read error %s: %s", src["path"], exc)
            return []

        # ── stitch multi-line entries (stack traces etc.) into one string ──
        merged = []
        pending = []

        for raw in lines:
            stripped = raw.strip()
            if not stripped:
                continue
            is_new_entry = any(pat.match(stripped) for pat in LOG_PATTERNS)
            if is_new_entry:
                if pending:
                    merged.append(" | ".join(pending)) 
                pending = [stripped]
            else:
                if pending:
                    pending.append(stripped)            
                else:
                    pending = [stripped]               

        if pending:
            merged.append(" | ".join(pending))       

        return merged
    
    def _maybe_truncate(self, src: Dict[str, Any]) -> None:
        path = src["path"]
        try:
            mb = os.path.getsize(path) / (1024 * 1024)
            if mb <= self.MAX_FILE_SIZE_MB:
                return
            log.warning("%.1f MB exceeds limit — truncating %s", mb, path)
            with open(path, "r", errors="replace") as fh:
                tail = fh.readlines()[-self.KEEP_LAST_LINES:]
            with open(path, "w") as fh:
                fh.writelines(tail)
            src["pos"] = os.path.getsize(path)
            log.info("Kept last %d lines in %s", len(tail), path)
        except Exception as exc:
            log.error("Truncate error %s: %s", path, exc)

    # ── main loop ─────────────────────────────────────────────────────────────

    def _seek_to_end(self) -> None:
        """Start from EOF so we don't re-ship historical data on startup."""
        for src in self.sources:
            if os.path.exists(src["path"]):
                src["pos"]   = os.path.getsize(src["path"])
                src["inode"] = os.stat(src["path"]).st_ino
                log.info("  [%s] %s  (offset %d)", src["log_type"], src["path"], src["pos"])
            else:
                log.warning("  [%s] NOT FOUND (will retry): %s", src["log_type"], src["path"])

    def run(self) -> None:
        if not self.sources:
            log.error("No log sources configured — set APP_LOG_PATH (and others) in .env")
            return
        if not self.es:
            log.error("Elasticsearch unavailable — aborting")
            return

        log.info("─── Starting log shipper ───")
        log.info("elasticsearch-py major version : %d", ES_VERSION)
        log.info("Sources to monitor:")
        self._seek_to_end()
        log.info("Poll interval: %.1fs | Bulk size: %d", self.POLL_INTERVAL, self.BULK_SIZE)
        log.info("────────────────────────────")

        while True:
            try:
                for src in self.sources:
                    if not os.path.exists(src["path"]):
                        continue
                    self._detect_rotation(src)
                    for raw in self._read_new(src):
                        doc = parse_line(raw)
                        if doc:
                            self._buffer(self._enrich(doc, src))
                    self._maybe_truncate(src)

                if self._buf:
                    self._flush()

                time.sleep(self.POLL_INTERVAL)

            except KeyboardInterrupt:
                log.info("Interrupted — flushing %d remaining docs …", len(self._buf))
                self._flush()
                log.info("Shutdown complete.")
                break
            except Exception as exc:
                log.error("Unhandled error in main loop: %s", exc, exc_info=True)
                time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ElasticSearchLogger().run()
