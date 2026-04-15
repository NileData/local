/** Internal job tracking -- stored in SQLite, not exposed directly via API */
export interface LocalJob {
  jobId: string;
  type: "query" | "import" | "export";
  status: "QUEUED" | "RUNNING" | "COMPLETED" | "FAILED" | "CANCELLED";
  engine: "spark";
  inputJson: string;
  resultJson?: string;
  resultPath?: string;
  errorMessage?: string;
  submittedAt: string;
  completedAt?: string;
  databaseName?: string;
  tableName?: string;
  rowCount?: number;
}

export type SparkStatus = "initializing" | "ready" | "unavailable";

export interface HealthResponse {
  status: "ok";
  engines: {
    spark: { status: SparkStatus; reason?: string; progress?: string };
  };
  sqliteVersion: string;
  dataDir: string;
}
