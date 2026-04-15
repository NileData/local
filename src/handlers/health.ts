import type { RequestHandler } from 'express';
import type { LocalConfig } from '../config/environment.js';
import type { SparkEngine } from '../engines/spark-engine.js';
import type { SQLiteService } from '../services/sqlite-service.js';
import type { HealthResponse } from '../types/local.js';

export function createHealthHandler(
  config: LocalConfig,
  sparkEngine: SparkEngine,
  sqliteService: SQLiteService,
): RequestHandler {
  return (_req, res) => {
    const sparkStatus = sparkEngine.status();
    const response: HealthResponse = {
      status: "ok",
      engines: {
        spark: {
          status: sparkStatus,
          ...(sparkStatus === "unavailable" && sparkEngine.getUnavailableReason()
            ? { reason: sparkEngine.getUnavailableReason() }
            : {}),
          ...(sparkStatus === "initializing" && sparkEngine.getProgressMessage()
            ? { progress: sparkEngine.getProgressMessage() }
            : {}),
        },
      },
      sqliteVersion: sqliteService.getVersion(),
      dataDir: config.local.dataDir,
    };

    res.status(200).json(response);
  };
}
