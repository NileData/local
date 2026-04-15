import type { SkillDefinition } from "../system-skills.js";

export const SKILL_CLOUD_TRANSFER: SkillDefinition = {
  name: "cloud-transfer",
  description: "Transfer data from S3, GCS, or Azure Blob Storage into local Nile tables.",
  modes: ["local"],
  content: `# Cloud Storage Transfer

Read data directly from cloud storage (S3, GCS, Azure Blob) when credentials are configured, or download first as a fallback.

## S3 Direct Read

When AWS credentials are configured (via environment or \`~/.aws/credentials\`):

\`\`\`python
def transform_data(spark):
    # PySpark reads S3 natively with hadoop-aws
    df = spark.read.parquet("s3a://bucket/path/data.parquet")
    return df
\`\`\`

### S3 Download-First Fallback

\`\`\`python
def transform_data(spark):
    import boto3, os

    s3 = boto3.client("s3")
    local_path = "/tmp/cloud_data.parquet"
    s3.download_file("bucket", "path/data.parquet", local_path)

    df = spark.read.parquet(f"file://{local_path}")
    return df
\`\`\`

## GCS (Google Cloud Storage)

\`\`\`python
def transform_data(spark):
    from google.cloud import storage
    import os

    client = storage.Client()
    bucket = client.bucket("my-bucket")
    blob = bucket.blob("path/data.csv")
    local_path = "/tmp/gcs_data.csv"
    blob.download_to_filename(local_path)

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(local_path)
    return df
\`\`\`

## Azure Blob Storage

\`\`\`python
def transform_data(spark):
    from azure.storage.blob import BlobServiceClient
    import os

    connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_service = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service.get_blob_client("container", "path/data.parquet")

    local_path = "/tmp/azure_data.parquet"
    with open(local_path, "wb") as f:
        f.write(blob_client.download_blob().readall())

    df = spark.read.parquet(f"file://{local_path}")
    return df
\`\`\`

## S3 Directory (Multiple Files)

\`\`\`python
def transform_data(spark):
    import boto3, os

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    os.makedirs("/tmp/cloud_data", exist_ok=True)
    for page in paginator.paginate(Bucket="bucket", Prefix="path/partitioned-data/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                local = f"/tmp/cloud_data/{os.path.basename(obj['Key'])}"
                s3.download_file("bucket", obj["Key"], local)

    df = spark.read.parquet("file:///tmp/cloud_data/")
    return df
\`\`\`

## Gotchas

- **Credentials** -- ensure cloud credentials are configured before attempting direct reads. Nile uses the host machine's credential chain.
- **s3a vs s3** -- use \`s3a://\` protocol for PySpark (hadoop-aws connector). Plain \`s3://\` may not work.
- **Network** -- cloud reads require internet access. Download-first is more reliable for unstable connections.
- **Large datasets** -- for datasets > 10GB, prefer direct read over download to avoid filling local disk.
- **Cross-region** -- reading from a different region incurs data transfer costs and higher latency.
`,
};
