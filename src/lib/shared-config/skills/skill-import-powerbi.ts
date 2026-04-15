import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_POWERBI: SkillDefinition = {
  name: "import-powerbi",
  description: "Import Power BI (.pbix) data models into Iceberg tables via PySpark.",
  content: `# Import Power BI Files (.pbix)

Power BI Desktop files are ZIP archives containing a data model, queries, and report definitions. Extract the embedded data model for analysis.

## Prerequisites

- No extra libraries required -- uses stdlib \`zipfile\` and \`json\` modules.
- The .pbix file must not be connected to a live Power BI service dataset.

## PySpark Recipe

\`\`\`python
def transform_data(spark):
    import zipfile, json, boto3

    s3 = boto3.client("s3")
    s3.download_file("bucket", "path/report.pbix", "/tmp/report.pbix")

    with zipfile.ZipFile("/tmp/report.pbix", "r") as z:
        # List contents to find data model
        print("PBIX contents:", z.namelist())

        # DataModelSchema contains table definitions and relationships
        if "DataModelSchema" in z.namelist():
            schema_bytes = z.read("DataModelSchema")
            # May be UTF-16 LE encoded
            schema_text = schema_bytes.decode("utf-16-le", errors="ignore")
            model = json.loads(schema_text)

            # Extract table metadata
            rows = []
            for table in model.get("model", {}).get("tables", []):
                for col in table.get("columns", []):
                    rows.append({
                        "table_name": table["name"],
                        "column_name": col["name"],
                        "data_type": col.get("dataType", "unknown"),
                        "is_hidden": col.get("isHidden", False),
                    })

            df = spark.createDataFrame(rows)
            return df
\`\`\`

### Extract M (Power Query) Expressions

\`\`\`python
def transform_data(spark):
    import zipfile, json, boto3

    s3 = boto3.client("s3")
    s3.download_file("bucket", "path/report.pbix", "/tmp/report.pbix")

    with zipfile.ZipFile("/tmp/report.pbix", "r") as z:
        schema_bytes = z.read("DataModelSchema")
        schema_text = schema_bytes.decode("utf-16-le", errors="ignore")
        model = json.loads(schema_text)

        rows = []
        for table in model.get("model", {}).get("tables", []):
            for partition in table.get("partitions", []):
                source = partition.get("source", {})
                rows.append({
                    "table_name": table["name"],
                    "partition_name": partition.get("name", ""),
                    "query_type": source.get("type", ""),
                    "expression": str(source.get("expression", "")),
                })

        df = spark.createDataFrame(rows)
        return df
\`\`\`

## Gotchas

- **No raw data** -- PBIX files with imported data store it in a compressed binary format (ABF/Vertipaq) that cannot be easily read. This skill extracts the schema/metadata, not row-level data.
- **Live connection** -- PBIX files using DirectQuery or live connections contain no embedded data at all.
- **Encoding** -- DataModelSchema is typically UTF-16 LE encoded. Decode accordingly.
- **Large files** -- PBIX files can be several GB. Download to /tmp with sufficient disk space.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
