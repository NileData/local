import type { SkillDefinition } from "../system-skills.js";

export const SKILL_EXPORT: SkillDefinition = {
  name: "export",
  description: "Export query results to CSV, Parquet, or Excel files from local Nile.",
  modes: ["local"],
  content: `# Data Export

Export query results from Nile to local files. Results are saved to the operations results directory.

## Output Directory

Exported files are saved to:
\`\`\`
~/.nile/operations/results/
\`\`\`

## Export as CSV

\`\`\`python
def transform_data(spark):
    import os

    source = spark.table("database.table_name")
    output_path = os.path.expanduser("~/.nile/operations/results/export.csv")

    source.toPandas().to_csv(output_path, index=False)
    print(f"Exported {source.count()} rows to {output_path}")

    return source  # return for Nile tracking
\`\`\`

## Export as Parquet

\`\`\`python
def transform_data(spark):
    import os

    source = spark.table("database.table_name")
    output_path = os.path.expanduser("~/.nile/operations/results/export.parquet")

    source.toPandas().to_parquet(output_path, index=False)
    print(f"Exported to {output_path}")

    return source
\`\`\`

## Export as Excel

\`\`\`python
def transform_data(spark):
    import os

    source = spark.table("database.table_name")
    output_path = os.path.expanduser("~/.nile/operations/results/export.xlsx")

    source.toPandas().to_excel(output_path, index=False, sheet_name="Data")
    print(f"Exported to {output_path}")

    return source
\`\`\`

## Export with Query Filter

\`\`\`python
def transform_data(spark):
    import os
    from pyspark.sql.functions import col

    source = spark.table("database.table_name")
    filtered = source.filter(col("status") == "active").select("id", "name", "amount")

    output_path = os.path.expanduser("~/.nile/operations/results/filtered_export.csv")
    filtered.toPandas().to_csv(output_path, index=False)
    print(f"Exported {filtered.count()} rows to {output_path}")

    return filtered
\`\`\`

## Gotchas

- **Memory** -- \`toPandas()\` collects all data to driver memory. For tables > 1M rows, export in partitions or use Spark's native write.
- **Excel row limit** -- Excel supports max ~1M rows per sheet. Split large exports across sheets.
- **File overwrite** -- these recipes overwrite existing files. Add timestamps to filenames for versioning.
- **Parquet preferred** -- for large exports, Parquet is significantly smaller and faster than CSV.
`,
};
