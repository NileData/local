#!/usr/bin/env python3
"""
Nile local Spark sidecar -- HTTP bridge to Spark Connect.

Connects to the Spark Connect gRPC server (sc://localhost:15002) and exposes
the same HTTP API that api-local expects. This replaces the old approach
where the sidecar owned the SparkSession directly.

Endpoints:
  GET  /health          - Returns sidecar status and Spark version
  POST /execute         - Executes SQL via spark.sql() (DDL and queries)
  POST /execute-python  - Executes PySpark code with `spark` pre-bound
  POST /stop            - Interrupts running operations (best-effort)
  GET  /table-path      - Returns the warehouse path for a given namespace.table
"""

import os
import sys
import json
import traceback
import decimal
import datetime
import subprocess
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from threading import Thread
from urllib.parse import urlparse, parse_qs

# Global state
spark_session = None
sidecar_status = "initializing"
sidecar_error = None

WAREHOUSE = os.environ.get('WAREHOUSE_PATH', '/warehouse')
HTTP_PORT = int(os.environ.get('HTTP_PORT', '3002'))
SPARK_CONNECT_URL = os.environ.get('SPARK_CONNECT_URL', 'sc://localhost:15002')
MAX_INLINE_ROWS = 10000
RESULTS_DIR = "/results"

DDL_PREFIXES = ('CREATE', 'DROP', 'ALTER', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'TRUNCATE')


def _is_spark_connect_alive():
    """Check if the Spark Connect JVM process is still running (not zombie/defunct)."""
    try:
        result = subprocess.run(
            ["ps", "-eo", "pid,stat,comm"],
            capture_output=True, text=True, timeout=5
        )
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 3 and "java" in parts[2].lower():
                stat = parts[1]
                if "Z" in stat:  # zombie
                    return False
                return True
        # No Java process found at all
        return False
    except Exception:
        return True  # Assume alive if we can't check


class SparkJsonEncoder(json.JSONEncoder):
    """JSON encoder that handles Spark/Python types not natively serializable."""
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, datetime.timedelta):
            return str(obj)
        if isinstance(obj, (bytes, bytearray)):
            return obj.hex()
        return super().default(obj)


def is_ddl(sql):
    """Check if SQL statement is a DDL/DML operation (no result set expected)."""
    first_word = sql.strip().upper().split()[0] if sql.strip() else ''
    return first_word in DDL_PREFIXES


def init_spark():
    """Connect to Spark Connect server via gRPC."""
    global spark_session, sidecar_status, sidecar_error
    try:
        from pyspark.sql import SparkSession
        spark_session = (
            SparkSession.builder
            .remote(SPARK_CONNECT_URL)
            .appName('NileLocal')
            .getOrCreate()
        )
        # Set default catalog to 'local' so 2-part names (db.table) work
        spark_session.sql("USE local")
        version = spark_session.version
        sidecar_status = "ready"
        print(f"[spark-sidecar] Connected to Spark Connect at {SPARK_CONNECT_URL}")
        print(f"[spark-sidecar] Spark version: {version}")
        print(f"[spark-sidecar] Default catalog set to 'local' (2-part names enabled)")
    except Exception as e:
        sidecar_status = "error"
        sidecar_error = str(e)
        print(f"[spark-sidecar] Failed to connect to Spark: {e}")
        traceback.print_exc()


def df_to_response(df, job_id="unknown"):
    """Convert a Spark DataFrame to the standard JSON response dict."""
    rows_raw = df.collect()
    columns = [
        {"name": field.name, "type": str(field.dataType)}
        for field in df.schema.fields
    ]
    col_names = [f.name for f in df.schema.fields]
    row_dicts = [{c: row[c] for c in col_names} for row in rows_raw]
    row_count = len(row_dicts)

    if row_count > MAX_INLINE_ROWS:
        os.makedirs(RESULTS_DIR, exist_ok=True)
        result_path = os.path.join(RESULTS_DIR, f"{job_id}.json")
        with open(result_path, "w") as f:
            json.dump({"columns": columns, "rows": row_dicts, "rowCount": row_count}, f, cls=SparkJsonEncoder)
        return {"columns": columns, "rows": [], "rowCount": row_count, "resultPath": result_path}

    return {"columns": columns, "rows": row_dicts, "rowCount": row_count}


class SparkHandler(BaseHTTPRequestHandler):
    """HTTP request handler -- bridges HTTP to Spark Connect."""

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            # Check if Spark Connect gRPC server is still alive (not zombie)
            spark_alive = _is_spark_connect_alive()
            if sidecar_status == "ready" and not spark_alive:
                self._send_json(503, {
                    "status": "error",
                    "version": "unknown",
                    "mode": "spark-connect",
                    "connectUrl": SPARK_CONNECT_URL,
                    "message": "Spark Connect gRPC server is no longer running",
                })
                return
            version = spark_session.version if (spark_session and spark_alive) else "unknown"
            self._send_json(200, {
                "status": sidecar_status,
                "version": version,
                "mode": "spark-connect",
                "connectUrl": SPARK_CONNECT_URL,
                "message": sidecar_error if sidecar_status == "error" else None,
            })
        elif parsed.path == "/table-path":
            self._handle_table_path(parsed)
        else:
            self._send_json(404, {"error": "Not found"})

    def do_POST(self):
        if self.path == "/execute":
            self._handle_execute()
        elif self.path == "/execute-python":
            self._handle_execute_python()
        elif self.path == "/stop":
            self._handle_stop()
        else:
            self._send_json(404, {"error": "Not found"})

    def _handle_table_path(self, parsed):
        """Return the filesystem path for a given namespace.table in the warehouse."""
        params = parse_qs(parsed.query)
        ns = params.get('namespace', [None])[0]
        tbl = params.get('table', [None])[0]
        if not ns or not tbl:
            self._send_json(400, {"error": "namespace and table are required"})
            return
        path = os.path.join(WAREHOUSE, ns, tbl)
        self._send_json(200, {"path": path})

    def _handle_execute(self):
        if sidecar_status != "ready" or spark_session is None:
            self._send_json(503, {"error": "Spark is not ready", "status": sidecar_status})
            return

        try:
            body = self._read_body()
            sql = body.get("sql", "").strip()
            job_id = body.get("jobId", "unknown")
            limit = body.get("limit")  # result set limit (matches cloud df.limit())

            if not sql:
                self._send_json(400, {"error": "Missing 'sql' field"})
                return

            # Tag-based cancellation: addTag/interruptTag allows targeted cancel
            # without disrupting concurrent imports or DDL on the shared engine.
            tag = f"job-{job_id}"
            spark_session.addTag(tag)
            try:
                # Spark Connect is lazy -- always collect to force execution.
                # DDL/DML (CREATE, INSERT, etc.) returns empty; SELECT returns data.
                df = spark_session.sql(sql)
                if is_ddl(sql):
                    df.collect()  # force execution (lazy in Spark Connect)
                    self._send_json(200, {"columns": [], "rows": [], "rowCount": 0})
                else:
                    if limit and isinstance(limit, int) and limit > 0:
                        df = df.limit(limit)
                    self._send_json(200, df_to_response(df, job_id))
            finally:
                spark_session.removeTag(tag)

        except Exception as e:
            self._send_json(400, {"error": str(e), "detail": traceback.format_exc()})

    def _handle_execute_python(self):
        """Execute PySpark/Python code with `spark` pre-bound to the SparkSession."""
        if sidecar_status != "ready" or spark_session is None:
            self._send_json(503, {"error": "Spark is not ready", "status": sidecar_status})
            return

        try:
            body = self._read_body()
            code = body.get("code", "").strip()
            job_id = body.get("jobId", "unknown")
            limit = body.get("limit")  # result set limit (matches cloud df.limit())

            if not code:
                self._send_json(400, {"error": "Missing 'code' field"})
                return

            # Tag-based cancellation for targeted interrupt support.
            tag = f"job-{job_id}"
            spark_session.addTag(tag)
            try:
                # Execute Python code with spark session in scope
                local_vars = {"spark": spark_session}
                exec(code, {"__builtins__": __builtins__}, local_vars)

                # Priority: _result > result > transform_data(spark) return value
                result_df = local_vars.get("_result", local_vars.get("result", None))

                # Auto-invoke transform_data(spark) if defined (standard DVC pattern)
                if result_df is None and callable(local_vars.get("transform_data")):
                    result_df = local_vars["transform_data"](spark_session)

                if result_df is not None and hasattr(result_df, 'collect'):
                    if limit and isinstance(limit, int) and limit > 0:
                        result_df = result_df.limit(limit)
                    self._send_json(200, df_to_response(result_df, job_id))
                else:
                    self._send_json(200, {"columns": [], "rows": [], "rowCount": 0})
            finally:
                spark_session.removeTag(tag)

        except Exception as e:
            self._send_json(400, {"error": str(e), "detail": traceback.format_exc()})

    def _handle_stop(self):
        """Best-effort targeted interruption using Spark Connect tag-based cancel."""
        try:
            body = self._read_body()
            job_id = body.get("jobId")
            if not job_id:
                self._send_json(400, {"error": "Missing 'jobId' field"})
                return
            if spark_session is not None:
                tag = f"job-{job_id}"
                try:
                    spark_session.interruptTag(tag)
                except Exception:
                    # Fallback to interruptAll for older Spark versions
                    try:
                        spark_session.interruptAll()
                    except Exception:
                        pass
            self._send_json(200, {"status": "interrupt_sent", "jobId": job_id})
        except Exception as e:
            self._send_json(500, {"error": str(e)})

    def _read_body(self):
        content_length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(content_length)
        return json.loads(raw.decode("utf-8"))

    def _send_json(self, status_code, data):
        # Pre-serialize BEFORE writing HTTP headers to avoid garbled responses
        # if json.dumps fails (e.g. non-serializable Spark types)
        body = json.dumps(data, cls=SparkJsonEncoder).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        print(f"[spark-sidecar] {format % args}")


def main():
    print(f"[spark-sidecar] Starting HTTP bridge on port {HTTP_PORT}...")
    print(f"[spark-sidecar] Connecting to Spark Connect at {SPARK_CONNECT_URL}")
    print(f"[spark-sidecar] Warehouse path: {WAREHOUSE}")

    # Connect to Spark in background thread
    init_thread = Thread(target=init_spark, daemon=True)
    init_thread.start()

    # Start HTTP server immediately (health endpoint available right away)
    server = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), SparkHandler)
    print(f"[spark-sidecar] HTTP server listening on port {HTTP_PORT}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("[spark-sidecar] Shutting down...")
        server.shutdown()


if __name__ == "__main__":
    main()
