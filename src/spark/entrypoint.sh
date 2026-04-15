#!/usr/bin/env bash
#
# Entrypoint: starts Spark Connect server, then HTTP bridge sidecar.
#

WAREHOUSE="${WAREHOUSE_PATH:-/warehouse}"
GRPC_PORT="${GRPC_PORT:-15002}"
HTTP_PORT="${HTTP_PORT:-3002}"
DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"

echo "[entrypoint] Starting Spark Connect server on port ${GRPC_PORT}..."
echo "[entrypoint] Iceberg warehouse: ${WAREHOUSE}"
echo "[entrypoint] Driver memory: ${DRIVER_MEMORY}"

# Iceberg's vectorized Parquet reader has been crashing the local ARM Spark
# Connect JVM during grouped aggregates on local tables. Disable it in local
# mode so these queries complete reliably instead of hanging the worker.

# Start Spark Connect server in background
/opt/spark/sbin/start-connect-server.sh \
  --master "local[*]" \
  --driver-memory "${DRIVER_MEMORY}" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.local.type=hadoop" \
  --conf "spark.sql.catalog.local.warehouse=${WAREHOUSE}" \
  --conf "spark.sql.defaultCatalog=local" \
  --conf "spark.sql.iceberg.vectorization.enabled=false" \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.connect.grpc.binding.port=${GRPC_PORT}" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" \
  --conf "spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS" \
  --conf "spark.hadoop.fs.wasbs.impl=org.apache.hadoop.fs.azure.NativeAzureFileSystem" &

# Wait for gRPC port to be ready
echo "[entrypoint] Waiting for Spark Connect gRPC on port ${GRPC_PORT}..."
TIMEOUT=120
ELAPSED=0
while true; do
  if python3 -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('127.0.0.1',${GRPC_PORT})); s.close()" 2>/dev/null; then
    echo "[entrypoint] Spark Connect ready on port ${GRPC_PORT}"
    break
  fi
  sleep 2
  ELAPSED=$((ELAPSED + 2))
  if [ "$ELAPSED" -ge "$TIMEOUT" ]; then
    echo "[entrypoint] Spark Connect did not start within ${TIMEOUT}s"
    exit 1
  fi
done

# Start HTTP bridge sidecar in foreground
# Pass the actual gRPC port so the sidecar connects to the right address
export SPARK_CONNECT_URL="sc://localhost:${GRPC_PORT}"
echo "[entrypoint] Starting HTTP bridge sidecar on port ${HTTP_PORT}..."
exec python3 /sidecar/spark_sidecar.py
