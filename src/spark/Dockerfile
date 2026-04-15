# Spark Connect Server + HTTP Bridge Sidecar
#
# Architecture:
#   1. Spark Connect gRPC server (port 15002) -- managed by start-connect-server.sh
#   2. HTTP bridge sidecar (port 3002) -- thin Python HTTP->Spark Connect proxy
#
# The HTTP bridge connects to Spark Connect via sc://localhost:15002 and
# exposes the same HTTP API that api-local expects.

FROM apache/spark:3.5.4-python3

USER root

ENV ICEBERG_VERSION=1.7.1

# Download Iceberg runtime JAR for Spark 3.5
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar

# Download Spark Connect JAR (required for 3.5.x; bundled in 4.0+)
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.5.4/spark-connect_2.12-3.5.4.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/spark-connect_2.12-3.5.4.jar

# Cloud connector JARs -- enable s3a://, gs://, wasbs:// reads
ENV HADOOP_AWS_VERSION=3.3.4
ENV GCS_CONNECTOR_VERSION=hadoop3-2.2.22
ENV HADOOP_AZURE_VERSION=3.3.4
ENV AWS_SDK_BUNDLE_VERSION=1.12.780

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/${GCS_CONNECTOR_VERSION}/gcs-connector-${GCS_CONNECTOR_VERSION}-shaded.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/${HADOOP_AZURE_VERSION}/hadoop-azure-${HADOOP_AZURE_VERSION}.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/hadoop-aws-*.jar /opt/spark/jars/aws-java-sdk-bundle-*.jar \
    /opt/spark/jars/gcs-connector-*.jar /opt/spark/jars/hadoop-azure-*.jar

# JDBC drivers -- enable postgres://, snowflake:// imports
ENV PG_JDBC_VERSION=42.7.4
ENV SF_JDBC_VERSION=3.18.0
ENV SF_SPARK_VERSION=2.16.0-spark_3.4

ADD https://jdbc.postgresql.org/download/postgresql-${PG_JDBC_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/${SF_JDBC_VERSION}/snowflake-jdbc-${SF_JDBC_VERSION}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/${SF_SPARK_VERSION}/spark-snowflake_2.12-${SF_SPARK_VERSION}.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/postgresql-*.jar /opt/spark/jars/snowflake-jdbc-*.jar \
    /opt/spark/jars/spark-snowflake_*.jar

# Install system deps for lxml, upgrade pip for manylinux_2_17 wheel support,
# then install Python packages. Pin grpcio==1.70.0 (has pre-built aarch64 py38 wheels)
RUN apt-get update && apt-get install -y --no-install-recommends libxml2-dev libxslt1-dev && \
    rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir "grpcio==1.70.0" "grpcio-status==1.70.0"
RUN pip install --no-cache-dir "pyspark[connect]==3.5.4" openpyxl lxml

# Ensure Python output is not buffered (critical for log visibility in containers)
ENV PYTHONUNBUFFERED=1

# Create directories with correct permissions for spark user (UID 185)
RUN mkdir -p /warehouse /results /sidecar && chown -R 185:0 /warehouse /results /sidecar

# Copy HTTP bridge sidecar and entrypoint
COPY spark_sidecar.py /sidecar/spark_sidecar.py
COPY entrypoint.sh /sidecar/entrypoint.sh
RUN chmod +x /sidecar/entrypoint.sh

EXPOSE 15002 3002

USER 185

ENTRYPOINT ["/sidecar/entrypoint.sh"]
