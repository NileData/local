import type { SkillDefinition } from "../system-skills.js";

export const SKILL_DATA_TRANSFORM: SkillDefinition = {
  name: "data-transform",
  description: "ETL transform patterns -- dedup, clean, join, pivot, unpivot, window functions, type casting.",
  content: `# ETL Transform Patterns

Common PySpark transformation recipes for data cleaning, reshaping, and enrichment. All examples use the \`transform_data(spark)\` pattern.

## Deduplication

\`\`\`python
def transform_data(spark):
    from pyspark.sql.functions import row_number, col, desc
    from pyspark.sql.window import Window

    source = spark.table("database.table_name")

    # Keep latest row per key
    window = Window.partitionBy("id").orderBy(desc("updated_at"))
    df = (
        source.withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    return df
\`\`\`

## String Cleaning

\`\`\`python
def transform_data(spark):
    from pyspark.sql.functions import col, trim, lower, regexp_replace, when

    source = spark.table("database.table_name")
    df = source.select(
        col("id"),
        trim(col("name")).alias("name"),
        lower(trim(col("email"))).alias("email"),
        regexp_replace(col("phone"), "[^0-9]", "").alias("phone_clean"),
        when(col("status").isNull(), "unknown").otherwise(col("status")).alias("status"),
    )
    return df
\`\`\`

## Join Tables

\`\`\`python
def transform_data(spark):
    orders = spark.table("database.orders")
    customers = spark.table("database.customers")

    df = orders.join(customers, orders.customer_id == customers.id, "left")
    return df
\`\`\`

## Pivot (Rows to Columns)

\`\`\`python
def transform_data(spark):
    from pyspark.sql.functions import sum

    source = spark.table("database.sales")
    df = (
        source.groupBy("product")
        .pivot("quarter", ["Q1", "Q2", "Q3", "Q4"])
        .agg(sum("revenue"))
    )
    return df
\`\`\`

## Unpivot (Columns to Rows)

\`\`\`python
def transform_data(spark):
    from pyspark.sql.functions import expr

    source = spark.table("database.wide_table")
    df = source.selectExpr(
        "id",
        "stack(3, 'col_a', col_a, 'col_b', col_b, 'col_c', col_c) as (metric, value)"
    )
    return df
\`\`\`

## Window Functions

\`\`\`python
def transform_data(spark):
    from pyspark.sql.functions import col, sum, avg, lag
    from pyspark.sql.window import Window

    source = spark.table("database.daily_metrics")
    window = Window.partitionBy("category").orderBy("date")

    df = source.select(
        "*",
        sum("amount").over(window).alias("running_total"),
        avg("amount").over(window.rowsBetween(-6, 0)).alias("7day_avg"),
        lag("amount", 1).over(window).alias("prev_day_amount"),
    )
    return df
\`\`\`

## Type Casting

\`\`\`python
def transform_data(spark):
    from pyspark.sql.functions import col, to_date, to_timestamp

    source = spark.table("database.raw_data")
    df = source.select(
        col("id").cast("long"),
        col("amount").cast("decimal(18,2)"),
        to_date(col("date_str"), "yyyy-MM-dd").alias("date_val"),
        to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss").alias("timestamp_val"),
        col("flag").cast("boolean"),
    )
    return df
\`\`\`

## Gotchas

- **Join skew** -- if one key has millions of rows, use salting or broadcast join: \`broadcast(small_df)\`.
- **Pivot cardinality** -- always pass explicit values list to \`pivot()\` to avoid a full scan.
- **Window frame** -- default window frame is unbounded when ORDER BY is absent. Always specify \`rowsBetween\` or \`rangeBetween\` for clarity.
- **Null handling in joins** -- null keys never match in joins. Filter or coalesce nulls before joining.
`,
};
