import { ComputeEngine } from './compute-engine.js';
import { SparkEngine } from './spark-engine.js';

export class EngineUnavailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EngineUnavailableError';
  }
}

/**
 * Singleton registry -- created in server.ts, injected into handlers.
 */
export class EngineRegistry {
  private spark: SparkEngine;

  constructor(spark: SparkEngine) {
    this.spark = spark;
  }

  resolve(_engine?: string, _defaultEngine?: string): ComputeEngine {
    if (this.spark.status() === "unavailable") {
      throw new EngineUnavailableError(
        "Spark is not available. Please restart Nile."
      );
    }
    return this.spark;
  }

  getSpark(): SparkEngine {
    return this.spark;
  }
}
