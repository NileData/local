import type { SkillDefinition } from "../system-skills.js";

export const SKILL_DATA_PROFILING: SkillDefinition = {
  name: "data-profiling",
  description: "Data quality profiling -- distributions, null counts, unique values, type inference for any table.",
  content: `# Data Quality Profiling

Analyze any Iceberg table or DataFrame for data quality: null counts, unique values, distributions, outliers, and type inference. Use these PySpark recipes to understand your data.

## Full Table Profile

\`\`\`python
def transform_data(spark):
    from pyspark.sql.functions import (
        col, count, countDistinct, sum as spark_sum, min as spark_min,
        max as spark_max, avg, stddev, length, when, isnan, isnull
    )

    source = spark.table("database.table_name")
    columns = source.columns
    total = source.count()

    rows = []
    for c in columns:
        col_type = str(source.schema[c].dataType)
        stats = source.agg(
            count(col(c)).alias("non_null"),
            countDistinct(col(c)).alias("distinct"),
            spark_sum(when(isnull(col(c)), 1).otherwise(0)).alias("null_count"),
        ).collect()[0]

        row = {
            "column_name": c,
            "data_type": col_type,
            "total_rows": total,
            "non_null_count": stats["non_null"],
            "null_count": stats["null_count"],
            "null_pct": round(stats["null_count"] / total * 100, 2) if total > 0 else 0,
            "distinct_count": stats["distinct"],
            "uniqueness_pct": round(stats["distinct"] / total * 100, 2) if total > 0 else 0,
        }
        rows.append(row)

    df = spark.createDataFrame(rows)
    return df
\`\`\`

## Numeric Column Statistics

\`\`\`python
def transform_data(spark):
    from pyspark.sql.functions import col, min, max, avg, stddev, percentile_approx

    source = spark.table("database.table_name")
    numeric_cols = [f.name for f in source.schema.fields
                    if str(f.dataType) in ("IntegerType", "LongType", "DoubleType", "FloatType", "DecimalType(38,18)")]

    rows = []
    for c in numeric_cols:
        stats = source.agg(
            min(col(c)).alias("min_val"),
            max(col(c)).alias("max_val"),
            avg(col(c)).alias("mean"),
            stddev(col(c)).alias("std_dev"),
            percentile_approx(col(c), 0.5).alias("median"),
        ).collect()[0]
        rows.append({
            "column_name": c,
            "min": float(stats["min_val"]) if stats["min_val"] is not None else None,
            "max": float(stats["max_val"]) if stats["max_val"] is not None else None,
            "mean": float(stats["mean"]) if stats["mean"] is not None else None,
            "std_dev": float(stats["std_dev"]) if stats["std_dev"] is not None else None,
            "median": float(stats["median"]) if stats["median"] is not None else None,
        })

    df = spark.createDataFrame(rows)
    return df
\`\`\`

## Value Frequency (Top N)

\`\`\`python
def transform_data(spark):
    from pyspark.sql.functions import col, count, desc

    source = spark.table("database.table_name")
    column_name = "status"  # change to target column

    df = (
        source.groupBy(col(column_name))
        .agg(count("*").alias("frequency"))
        .orderBy(desc("frequency"))
        .limit(20)
    )
    return df
\`\`\`

## Gotchas

- **Performance** -- profiling scans the full table. For large tables, sample first: \`source.sample(0.01)\`.
- **String length stats** -- add \`avg(length(col(c)))\` for string columns to detect data issues.
- **Cardinality** -- \`countDistinct\` is expensive on large datasets. Use \`approx_count_distinct\` for estimates.
- **Nulls vs empty strings** -- check for both: \`isnull(col(c)) | (col(c) == "")\`.
`,
};
