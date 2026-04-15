import type { SkillDefinition } from "../system-skills.js";

export const SKILL_IMPORT_GEOJSON: SkillDefinition = {
  name: "import-geojson",
  description: "Import GeoJSON files into Iceberg tables via PySpark.",
  content: `# Import GeoJSON Files

GeoJSON files containing geospatial features (points, lines, polygons). Convert geometry to WKT strings for Iceberg storage.

## Prerequisites

- \`geopandas\` optional (simplifies geometry handling).
- Without geopandas: use stdlib \`json\` module to parse features manually.

## PySpark Recipe (with geopandas)

\`\`\`python
def transform_data(spark):
    import geopandas as gpd

    gdf = gpd.read_file("s3://bucket/path/data.geojson")
    # Convert geometry to WKT string for Iceberg storage
    gdf["geometry_wkt"] = gdf.geometry.to_wkt()
    pandas_df = gdf.drop(columns=["geometry"])

    df = spark.createDataFrame(pandas_df)
    return df
\`\`\`

### Without geopandas (stdlib only)

\`\`\`python
def transform_data(spark):
    import json, boto3
    from pyspark.sql import Row

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket="bucket", Key="path/data.geojson")
    geojson = json.loads(obj["Body"].read().decode("utf-8"))

    rows = []
    for feature in geojson["features"]:
        row = {**feature["properties"]}
        row["geometry_type"] = feature["geometry"]["type"]
        row["geometry_json"] = json.dumps(feature["geometry"])
        rows.append(row)

    df = spark.createDataFrame(rows)
    return df
\`\`\`

## Gotchas

- **Geometry storage** -- Iceberg has no native geometry type. Store as WKT string or GeoJSON string.
- **CRS** -- GeoJSON spec requires WGS84 (EPSG:4326). If your data uses a different CRS, reproject with geopandas.
- **Nested properties** -- some GeoJSON files have nested property objects. Flatten before creating DataFrame.
- **Large files** -- GeoJSON is verbose. For files > 500MB, consider converting to Parquet with geopandas first.
- **Null geometries** -- features with null geometry are valid GeoJSON. Handle with a null check.

## Verification

\`\`\`python
df.printSchema()
df.show(5, truncate=False)
print(f"Row count: {df.count()}")
\`\`\`
`,
};
