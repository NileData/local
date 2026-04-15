export interface QueryResult {
  columns: Array<{ name: string; type: string }>;
  rows: Record<string, unknown>[];
  rowCount: number;
}

export interface ComputeEngine {
  readonly name: "spark";
  executeSQL(sql: string, database?: string, jobId?: string): Promise<QueryResult>;
  executePython?(code: string, jobId?: string): Promise<QueryResult>;
  cancel(jobId: string): Promise<void>;
  status(): "ready" | "initializing" | "unavailable";
}
