import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_CONFIG: SkillDefinition = {
  name: "import-config",
  description: "Import YAML/TOML/INI configuration files into Iceberg tables via PySpark.",
  content: `# Import Configuration Files (YAML/TOML/INI)

Parse configuration files into tabular format. Useful for auditing config drift, tracking changes, or centralizing settings from multiple services.

## Prerequisites

- No extra libraries required -- Python stdlib includes \`configparser\` (INI) and \`tomllib\` (TOML, Python 3.11+).
- For YAML: \`pyyaml\` (\`pip install pyyaml\`), commonly pre-installed in Spark environments.

## PySpark Recipe (YAML)

\`\`\`python
def transform_data(spark):
    import yaml, boto3

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket="bucket", Key="path/config.yaml")
    config = yaml.safe_load(obj["Body"].read().decode("utf-8"))

    # Flatten nested config into key-value rows
    def flatten(data, prefix=""):
        rows = []
        for key, value in data.items():
            full_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                rows.extend(flatten(value, full_key))
            else:
                rows.append({"key": full_key, "value": str(value), "type": type(value).__name__})
        return rows

    rows = flatten(config)
    df = spark.createDataFrame(rows)
    return df
\`\`\`

### TOML

\`\`\`python
def transform_data(spark):
    import tomllib, boto3  # Python 3.11+ stdlib

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket="bucket", Key="path/config.toml")
    config = tomllib.loads(obj["Body"].read().decode("utf-8"))

    def flatten(data, prefix=""):
        rows = []
        for key, value in data.items():
            full_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                rows.extend(flatten(value, full_key))
            else:
                rows.append({"key": full_key, "value": str(value)})
        return rows

    df = spark.createDataFrame(flatten(config))
    return df
\`\`\`

### INI

\`\`\`python
def transform_data(spark):
    import configparser, boto3

    s3 = boto3.client("s3")
    s3.download_file("bucket", "path/config.ini", "/tmp/config.ini")

    parser = configparser.ConfigParser()
    parser.read("/tmp/config.ini")

    rows = []
    for section in parser.sections():
        for key, value in parser.items(section):
            rows.append({"section": section, "key": key, "value": value})

    df = spark.createDataFrame(rows)
    return df
\`\`\`

## Gotchas

- **Nested structures** -- YAML/TOML support deep nesting. The flatten function converts to dot-separated keys.
- **Lists** -- array values are converted to string representation. For list items as separate rows, iterate explicitly.
- **YAML anchors** -- \`yaml.safe_load\` resolves anchors/aliases automatically.
- **Multi-document YAML** -- use \`yaml.safe_load_all()\` for files with multiple \`---\` separated documents.
- **TOML on older Python** -- Python < 3.11 needs \`pip install tomli\` instead of stdlib \`tomllib\`.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
