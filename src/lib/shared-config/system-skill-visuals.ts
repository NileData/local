export const SYSTEM_VISUALS_SKILL = {
  name: "visuals",
  description: "Generate charts and visualizations from query results.",
  content: `# Inline Visualizations (show_chart)

You can display charts directly in the chat using the \`show_chart\` tool.

## When to Use show_chart

Use \`show_chart\` when:
- User asks for a visualization (chart, graph, plot)
- User asks "show me" data in a visual format
- Data would be better understood visually (comparisons, trends, proportions)
- User asks for top-N, rankings, distributions, or time series

## Visualization Workflow

1. Execute query with aggregation:
   - For bar charts: \`GROUP BY\` category and aggregate metrics
   - For line charts: \`ORDER BY\` the date or time column
   - For pie charts: \`GROUP BY\` category with a single metric
   - Include \`LIMIT\` for large datasets
2. Call \`get_query_results\` to retrieve data
3. Transform the 2D result array into an array of objects
4. Call \`show_chart\` with the transformed data and chart config

## Chart Type Selection

| Data Pattern | Chart Type | Example |
|--------------|------------|---------|
| Categories + values | bar | Sales by region |
| Time series | line | Revenue over months |
| Part of whole | pie | Market share |
| Two numeric variables | scatter | Price vs quantity |
| Trends with magnitude | area | Cumulative sales |
| Distribution of values | histogram | Salary distribution |
| Statistical summary | boxplot | Sales by region (with outliers) |

## Multi-Series Patterns

### Use yAxisColumns

Use \`yAxisColumns\` when comparing different numeric metrics.

Example data:
\`[{month: 'Jan', revenue: 100, costs: 60, profit: 40}]\`

Configuration:
\`yAxisColumns: ['revenue', 'costs', 'profit']\`

### Use groupBy

Use \`groupBy\` when comparing the same metric across categories.

Example data:
\`[{month: 'Jan', shop: 'A', sales: 100}, {month: 'Jan', shop: 'B', sales: 150}]\`

Configuration:
\`groupBy: 'shop', yAxis: 'sales'\`

### Combine Both

Use both when comparing multiple metrics across categories.

Configuration:
\`yAxisColumns: ['revenue', 'costs'], groupBy: 'region'\`

## Examples

### Bar Chart

\`\`\`json
{
  "vizType": "bar",
  "title": "Top 10 Products by Revenue",
  "data": [
    { "product_name": "Widget A", "revenue": 50000 },
    { "product_name": "Widget B", "revenue": 35000 }
  ],
  "xAxis": "product_name",
  "yAxis": "revenue",
  "options": { "layout": "horizontal" }
}
\`\`\`

### Line Chart

\`\`\`json
{
  "vizType": "line",
  "title": "Monthly Sales Trend",
  "data": [
    { "month": "2024-01", "sales": 10000 },
    { "month": "2024-02", "sales": 12000 }
  ],
  "xAxis": "month",
  "yAxis": "sales",
  "options": { "curved": true }
}
\`\`\`

### Pie Chart

\`\`\`json
{
  "vizType": "pie",
  "title": "Revenue by Category",
  "data": [
    { "category": "Electronics", "revenue": 50000 },
    { "category": "Clothing", "revenue": 30000 }
  ],
  "xAxis": "category",
  "yAxis": "revenue",
  "options": { "donut": true }
}
\`\`\`

## Important Rules

1. Aggregate data in SQL, not after the fact
2. Use human-readable labels for axes
3. Keep limits reasonable
4. Use clear chart titles
5. Transform query results into arrays of objects before calling \`show_chart\`
6. Table charts are temporarily disabled
7. Use \`histogram\` for continuous distributions
8. Use \`boxplot\` for quartiles, spread, and outlier-focused views
`,
};
