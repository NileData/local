import { mkdirSync, writeFileSync } from 'fs';
import { join } from 'path';
import type { LocalConfig } from '../config/environment.js';
import type { SparkEngine } from '../engines/spark-engine.js';
import type { SQLiteService } from './sqlite-service.js';

type QueryLanguage = 'sql' | 'python' | 'pyspark';

interface QueuedQueryJob {
  jobId: string;
  sql: string;
  language: QueryLanguage;
  resultSetLimit?: number;
}

interface QueryRuntimeState {
  workerId?: string;
  queuePosition?: number;
  queuedCountAhead?: number;
}

interface QueryWorker {
  id: string;
  engine: SparkEngine;
  state: 'idle' | 'starting' | 'busy';
  currentJobId?: string;
}

export class QueryWorkerPool {
  private readonly workers: QueryWorker[];
  private readonly queue: QueuedQueryJob[] = [];
  private readonly runtimeState = new Map<string, QueryRuntimeState>();
  private processing = false;

  constructor(
    private readonly config: LocalConfig,
    private readonly sqliteService: SQLiteService,
    sharedEngine: SparkEngine,
  ) {
    // All workers share the single Spark engine. Spark Connect's gRPC server
    // handles concurrent queries natively via ThreadingHTTPServer + local[*].
    this.workers = Array.from({ length: config.compute.maxConcurrentQueries }, (_, index) => ({
      id: `local-query-worker-${index + 1}`,
      engine: sharedEngine,
      state: 'idle' as const,
    }));
  }

  submit(job: QueuedQueryJob): void {
    this.queue.push(job);
    this.updateQueueMetadata();
    this.scheduleProcessQueue();
  }

  getRuntimeState(jobId: string): QueryRuntimeState | undefined {
    return this.runtimeState.get(jobId);
  }

  async cancel(jobId: string): Promise<void> {
    const queueIndex = this.queue.findIndex((job) => job.jobId === jobId);
    if (queueIndex >= 0) {
      this.queue.splice(queueIndex, 1);
      this.runtimeState.delete(jobId);
      this.updateQueueMetadata();
      this.sqliteService.updateJob(jobId, {
        status: 'CANCELLED',
        completedAt: new Date().toISOString(),
      });
      return;
    }

    const worker = this.workers.find((candidate) => candidate.currentJobId === jobId);
    if (!worker) {
      return;
    }

    this.runtimeState.delete(jobId);
    this.sqliteService.updateJob(jobId, {
      status: 'CANCELLED',
      completedAt: new Date().toISOString(),
    });

    await worker.engine.cancel(jobId);
    worker.currentJobId = undefined;
    worker.state = 'idle';
    this.scheduleProcessQueue();
  }

  async shutdown(): Promise<void> {
    // Do not shutdown the shared engine -- server.ts owns its lifecycle.
    // Just drain the queue so no new work starts.
    this.queue.length = 0;
  }

  private scheduleProcessQueue(): void {
    void this.processQueue();
  }

  private async processQueue(): Promise<void> {
    if (this.processing) {
      return;
    }

    this.processing = true;

    try {
      while (this.queue.length > 0) {
        const worker = this.workers.find((candidate) => candidate.state === 'idle');
        if (!worker) {
          break;
        }

        const nextJob = this.queue.shift();
        if (!nextJob) {
          break;
        }
        this.updateQueueMetadata();

        const persistedJob = this.sqliteService.getJob(nextJob.jobId);
        if (!persistedJob || persistedJob.status === 'CANCELLED') {
          continue;
        }

        worker.currentJobId = nextJob.jobId;
        worker.state = 'starting';
        this.runtimeState.set(nextJob.jobId, { workerId: worker.id });
        this.sqliteService.updateJob(nextJob.jobId, { status: 'RUNNING' });
        void this.runJobOnWorker(worker, nextJob);
      }
    } finally {
      this.processing = false;
    }
  }

  private async runJobOnWorker(worker: QueryWorker, job: QueuedQueryJob): Promise<void> {
    try {
      await this.ensureWorkerReady(worker);

      const persistedJob = this.sqliteService.getJob(job.jobId);
      if (!persistedJob || persistedJob.status === 'CANCELLED') {
        return;
      }

      worker.state = 'busy';

      const limit = job.resultSetLimit;
      const result = job.language === 'python' || job.language === 'pyspark'
        ? await worker.engine.executePython(job.sql, job.jobId, limit)
        : await worker.engine.executeSQL(job.sql, undefined, job.jobId, limit);

      const latestJob = this.sqliteService.getJob(job.jobId);
      if (!latestJob || latestJob.status === 'CANCELLED') {
        return;
      }

      try {
        this.sqliteService.insertQueryHistory(job.sql, null, result.rowCount);
      } catch {
        // Non-critical history write.
      }

      const resultsDir = join(this.config.local.dataDir, 'operations', 'results');
      mkdirSync(resultsDir, { recursive: true });
      const resultPath = join(resultsDir, `${job.jobId}.json`);
      writeFileSync(resultPath, JSON.stringify(result));

      this.sqliteService.updateJob(job.jobId, {
        status: 'COMPLETED',
        resultPath,
        rowCount: result.rowCount,
        completedAt: new Date().toISOString(),
      });
    } catch (error) {
      const latestJob = this.sqliteService.getJob(job.jobId);
      if (!latestJob || latestJob.status === 'CANCELLED') {
        return;
      }

      this.sqliteService.updateJob(job.jobId, {
        status: 'FAILED',
        errorMessage: error instanceof Error ? error.message : String(error),
        completedAt: new Date().toISOString(),
      });
    } finally {
      this.runtimeState.delete(job.jobId);
      worker.currentJobId = undefined;
      worker.state = 'idle';
      this.scheduleProcessQueue();
    }
  }

  private async ensureWorkerReady(worker: QueryWorker): Promise<void> {
    if (worker.engine.status() === 'ready') {
      return;
    }
    // Shared engine is initialized by server.ts at startup.
    // Don't attempt init/recycle here -- that would restart the main container.
    throw new Error(
      `Spark engine is not ready (status: ${worker.engine.status()}). ` +
      'Wait for engine initialization or restart the app.',
    );
  }

  private updateQueueMetadata(): void {
    this.queue.forEach((job, index) => {
      this.runtimeState.set(job.jobId, {
        queuePosition: index + 1,
        queuedCountAhead: index,
      });
    });
  }

}
