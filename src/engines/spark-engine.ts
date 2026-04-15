import { exec as execCb, spawn as nodeSpawn } from 'child_process';
import { promisify } from 'util';
import { dirname, join } from 'path';
import { homedir } from 'os';
import { mkdirSync, existsSync, writeFileSync, readFileSync, unlinkSync } from 'fs';
import { fileURLToPath } from 'url';
import { ComputeEngine, QueryResult } from './compute-engine.js';
import type { SparkStatus } from '../types/local.js';

const exec = promisify(execCb);
const __dirname = dirname(fileURLToPath(import.meta.url));

/** 10 minutes -- long enough for large CSV imports with inferSchema + CTAS */
const SPARK_QUERY_TIMEOUT_MS = 600_000;

/**
 * Resolve the Podman CLI binary path.
 * Priority:
 *   1. PODMAN_BIN_DIR env (bundled standalone binaries from Tauri app — macOS)
 *   2. Platform-specific common install locations
 *   3. Fallback: assume podman is in PATH
 */
function resolvePodmanBin(): string {
  // Bundled standalone binaries (macOS rootless — no admin needed)
  if (process.env['PODMAN_BIN_DIR']) {
    const candidate = join(process.env['PODMAN_BIN_DIR'], process.platform === 'win32' ? 'podman.exe' : 'podman');
    if (existsSync(candidate)) {
      return candidate;
    }
  }

  if (process.platform === 'win32') {
    const candidate = join(
      process.env['ProgramFiles'] ?? 'C:\\Program Files',
      'RedHat', 'Podman', 'podman.exe'
    );
    if (existsSync(candidate)) {
      return `"${candidate}"`;
    }
  } else if (process.platform === 'darwin') {
    // macOS: check .pkg install path, then Homebrew paths
    const candidates = [
      '/opt/podman/bin/podman',
      '/opt/homebrew/bin/podman',
      '/usr/local/bin/podman',
    ];
    for (const candidate of candidates) {
      if (existsSync(candidate)) {
        return candidate;
      }
    }
  }
  // Linux or fallback: assume podman is in PATH
  return 'podman';
}

/** Resolved lazily; updated after install so all subsequent commands use the correct path. */
let PODMAN_BIN = resolvePodmanBin();

const SPARK_IMAGE = 'nile-spark-connect:latest';
const HEALTH_POLL_INTERVAL_MS = 2000;
const HEALTH_TIMEOUT_MS = 600000;

/** Response shape from the /execute endpoint */
interface SparkExecuteResponse {
  columns: Array<{ name: string; type: string }>;
  rows: Record<string, unknown>[];
  rowCount: number;
  resultPath?: string;
  error?: string;
  detail?: string;
}

/** Response shape from the /table-path endpoint */
interface SparkTablePathResponse {
  path: string;
  error?: string;
}

interface SparkEngineOptions {
  containerName?: string;
  managePodmanMachineOnShutdown?: boolean;
}

export class SparkEngine implements ComputeEngine {
  readonly name = "spark" as const;
  private currentStatus: SparkStatus = "initializing";
  private unavailableReason?: string;
  private progressMessage?: string;
  private sparkPort: number;
  private dataDir: string;
  private warehouseDir: string;
  private driverMemoryGb: number;
  private containerName: string;
  private managePodmanMachineOnShutdown: boolean;

  constructor(sparkPort: number, dataDir: string, driverMemoryGb?: number, options: SparkEngineOptions = {}) {
    this.sparkPort = sparkPort;
    this.dataDir = dataDir;
    this.warehouseDir = join(dataDir, 'data-lake');
    this.driverMemoryGb = driverMemoryGb ?? 1;
    this.containerName = options.containerName ?? 'nile-spark-local';
    this.managePodmanMachineOnShutdown = options.managePodmanMachineOnShutdown ?? true;
  }

  private setProgress(message: string): void {
    this.progressMessage = message;
    console.log(`[spark-engine] ${message}`);
  }

  async init(): Promise<void> {
    console.log(`[spark-engine] init() starting (platform=${process.platform}, port=${this.sparkPort})`);
    console.log(`[spark-engine] PODMAN_BIN_DIR=${process.env['PODMAN_BIN_DIR'] ?? '(not set)'}`);
    console.log(`[spark-engine] PODMAN_INSTALLER_PATH=${process.env['PODMAN_INSTALLER_PATH'] ?? '(not set)'}`);
    console.log(`[spark-engine] PODMAN_BIN resolved to: ${PODMAN_BIN}`);
    console.log(`[spark-engine] warehouseDir: ${this.warehouseDir}`);

    try {
      // Ensure warehouse directory exists
      mkdirSync(this.warehouseDir, { recursive: true });

      // Check Podman availability (installs if needed)
      this.setProgress('Checking runtime');
      const podmanAvailable = await this.detectPodman();
      if (!podmanAvailable) {
        this.currentStatus = "unavailable";
        return;
      }

      // Ensure Podman machine is initialized and running (Windows/macOS)
      let machineWasRestarted = false;
      if (process.platform === 'win32' || process.platform === 'darwin') {
        const machineResult = await this.ensurePodmanMachine();
        if (!machineResult) {
          this.currentStatus = "unavailable";
          return;
        }
        machineWasRestarted = machineResult === 'started';
      }

      // If the VM was (re)started, any previous containers are gone — force-clean
      // stale container state to avoid port conflicts from dead containers.
      if (machineWasRestarted) {
        console.log('[spark-engine] VM was (re)started, cleaning stale containers');
        await exec(`${PODMAN_BIN} rm -f ${this.containerName}`, { env: this.getPodmanEnv(), timeout: 10000 }).catch(() => {});
      }

      // Container lifecycle: reuse running > start stopped > run existing image > build + run
      this.setProgress('Starting compute');
      await this.ensureContainer();
    } catch (err) {
      this.currentStatus = "unavailable";
      this.unavailableReason = err instanceof Error ? err.message : String(err);
    }
  }

  /**
   * Build exec environment options for Podman commands.
   * On Windows (Git Bash / MSYS2), volume mount paths like /warehouse get
   * mangled to C:\Program Files\Git\warehouse. Setting MSYS_NO_PATHCONV=1
   * prevents this.
   */
  private getPodmanEnv(): Record<string, string> {
    const env = { ...process.env } as Record<string, string>;
    if (process.platform === 'win32') {
      env['MSYS_NO_PATHCONV'] = '1';
    }
    return env;
  }

  /**
   * Ensure Podman machine is initialized and running.
   * On Windows/macOS, Podman runs containers inside a Linux VM.
   * The binary alone is not enough — the machine must be booted.
   *
   * Returns 'already-running' if the VM was already up, or 'started' if we
   * had to (re)start it. The caller uses this to decide whether stale
   * containers need force-cleanup (port bindings die when the VM restarts).
   */
  private async ensurePodmanMachine(): Promise<false | 'already-running' | 'started'> {
    const env = this.getPodmanEnv();
    const bin = PODMAN_BIN;

    // Check if a machine already exists
    let machineExists = false;
    let reportedRunning = false;
    try {
      const { stdout } = await exec(`${bin} machine info --format json`, { env, timeout: 15000 });
      const info = JSON.parse(stdout);
      if (info.Host?.MachineCount > 0) {
        machineExists = true;
        const { stdout: listOut } = await exec(`${bin} machine list --format json`, { env, timeout: 15000 });
        const machines = JSON.parse(listOut);
        reportedRunning = Array.isArray(machines) && machines.some((m: { Running?: boolean }) => m.Running);
      }
    } catch {
      // machine info not available — older podman or no machine yet
    }

    // If reported running, verify the VM is actually responsive.
    // vfkit can be killed externally (Activity Monitor, kill) leaving
    // podman's state file stale — it still says "Running" but the VM is dead.
    if (machineExists && reportedRunning) {
      try {
        await exec(`${bin} info --format json`, { env, timeout: 10000 });
        console.log('[spark-engine] Podman machine verified running');
        return 'already-running';
      } catch {
        console.log('[spark-engine] Podman machine reports running but is unresponsive — restarting');
        // Force stop to clear stale state before restarting
        await exec(`${bin} machine stop -f`, { env, timeout: 15000 }).catch(() => {});
        reportedRunning = false;
      }
    }

    // Machine exists but not running — start it
    if (machineExists && !reportedRunning) {
      this.setProgress('Starting virtual machine');
      try {
        await exec(`${bin} machine start`, { env, timeout: 300000 });
        console.log('[spark-engine] Podman machine started');
        return 'started';
      } catch (startErr) {
        const msg = startErr instanceof Error ? startErr.message : String(startErr);
        if (msg.includes('already running')) {
          return 'already-running';
        }
        console.error('[spark-engine] Failed to start Podman machine:', msg);
        this.unavailableReason = 'Failed to start Podman machine. Try running "podman machine start" manually.';
        return false;
      }
    }

    // No machine exists — initialize one
    this.setProgress('Setting up virtual machine');
    try {
      await exec(`${bin} machine init`, { env, timeout: 600000 });
      console.log('[spark-engine] Podman machine initialized');
    } catch (initErr) {
      const msg = initErr instanceof Error ? initErr.message : String(initErr);
      // "machine already exists" is fine
      if (!msg.includes('already exists')) {
        console.error('[spark-engine] Failed to init Podman machine:', msg);
        this.unavailableReason = 'Failed to initialize Podman machine. Try running "podman machine init" manually.';
        return false;
      }
    }

    // Start the machine
    this.setProgress('Starting virtual machine');
    try {
      await exec(`${bin} machine start`, { env, timeout: 300000 });
      console.log('[spark-engine] Podman machine started');
      return 'started';
    } catch (startErr) {
      const msg = startErr instanceof Error ? startErr.message : String(startErr);
      if (msg.includes('already running')) {
        return 'already-running';
      }
      console.error('[spark-engine] Failed to start Podman machine:', msg);
      this.unavailableReason = 'Failed to start Podman machine. Try running "podman machine start" manually.';
      return false;
    }
  }

  private async detectPodman(): Promise<boolean> {
    // Re-resolve every time — binary may have been installed since module load
    PODMAN_BIN = resolvePodmanBin();
    console.log(`[spark-engine] detectPodman: resolved binary = ${PODMAN_BIN}`);

    // If using bundled binaries, set up containers.conf so podman machine
    // can find gvproxy and vfkit in our bundle directory
    if (process.env['PODMAN_BIN_DIR']) {
      this.ensurePodmanConfig(process.env['PODMAN_BIN_DIR']);
    }

    try {
      const { stdout } = await exec(`${PODMAN_BIN} --version`, {
        env: this.getPodmanEnv(),
      });
      console.log(`[spark-engine] Podman found: ${stdout.trim()}`);
      return true;
    } catch (err) {
      console.log(`[spark-engine] Podman not found at ${PODMAN_BIN}: ${err instanceof Error ? err.message : String(err)}`);
      // Windows: try auto-install from bundled .exe installer (requires UAC)
      // macOS: binaries are bundled directly — no install step needed
      if (process.platform === 'win32') {
        console.log('[spark-engine] Attempting auto-install from bundled installer...');
        const installed = await this.tryAutoInstallPodman();
        if (installed) return true;
      }
      this.unavailableReason = 'Container runtime is not available. Install Podman (https://podman.io).';
      console.log(`[spark-engine] Podman unavailable: ${this.unavailableReason}`);
      return false;
    }
  }

  /**
   * Ensure containers.conf points to our bundled helper binaries (gvproxy, vfkit).
   * Podman machine needs these to create/start the Linux VM. Without this config,
   * podman looks in /usr/local/lib/podman/ which won't exist for bundled installs.
   *
   * Only writes helper_binaries_dir if not already configured — merges, doesn't overwrite.
   */
  private ensurePodmanConfig(binDir: string): void {
    const configDir = join(homedir(), '.config', 'containers');
    const configPath = join(configDir, 'containers.conf');

    mkdirSync(configDir, { recursive: true });

    const helperLine = `helper_binaries_dir = ["${binDir}"]`;

    if (existsSync(configPath)) {
      const content = readFileSync(configPath, 'utf-8');
      if (content.includes('helper_binaries_dir')) {
        // Already configured — check if our path is included
        if (content.includes(binDir)) {
          console.log('[spark-engine] containers.conf already has our helper_binaries_dir');
          return;
        }
        // User has their own config — don't overwrite, just log
        console.log('[spark-engine] containers.conf has helper_binaries_dir set by user, not overwriting');
        return;
      }
      // Append to existing [engine] section if present, otherwise add it
      if (content.includes('[engine]')) {
        const updated = content.replace('[engine]', `[engine]\n${helperLine}`);
        writeFileSync(configPath, updated);
      } else {
        const updated = content + `\n[engine]\n${helperLine}\n`;
        writeFileSync(configPath, updated);
      }
    } else {
      writeFileSync(configPath, `[engine]\n${helperLine}\n`);
    }
    console.log(`[spark-engine] Wrote containers.conf: ${helperLine}`);
  }

  /**
   * Attempt to install Podman from the bundled installer (Windows only).
   * macOS uses bundled standalone binaries — no install step needed.
   *
   * Only attempts once per data-dir — writes a marker file on failure/decline
   * to avoid prompting UAC elevation on every app launch.
   */
  private async tryAutoInstallPodman(): Promise<boolean> {
    // Check if we already tried and failed — don't nag the user
    const markerPath = join(this.dataDir, '.podman-install-attempted');
    if (existsSync(markerPath)) {
      const content = readFileSync(markerPath, 'utf-8');
      if (content.startsWith('installed=')) {
        // Podman was previously installed but is now gone — allow re-install
        console.log('[spark-engine] Podman was installed previously but not found now, retrying install');
        unlinkSync(markerPath);
      } else {
        // Previous install attempt failed — don't nag with UAC again
        console.log('[spark-engine] Podman install previously failed, skipping (delete ~/.nile/.podman-install-attempted to retry)');
        return false;
      }
    }

    // Look for bundled Windows installer
    const candidates: string[] = [];
    if (process.env['PODMAN_INSTALLER_PATH']) {
      candidates.push(process.env['PODMAN_INSTALLER_PATH']);
    }
    candidates.push(join(__dirname, '..', 'podman', 'podman-setup.exe'));
    candidates.push(join(__dirname, 'podman', 'podman-setup.exe'));

    let installerPath: string | null = null;
    for (const c of candidates) {
      if (existsSync(c)) {
        installerPath = c;
        break;
      }
    }

    if (!installerPath) {
      console.log('[spark-engine] No bundled Podman installer found, skipping auto-install');
      return false;
    }

    this.setProgress('Installing runtime');
    console.log(`[spark-engine] Installing Podman from bundled installer: ${installerPath}`);
    try {
      // Podman uses a WiX Burn bootstrapper.
      // Correct silent flags: /quiet /install /norestart
      await exec(
        `powershell -Command "Start-Process '${installerPath.replace(/'/g, "''")}' -ArgumentList '/quiet','/install','/norestart' -Verb RunAs -Wait"`,
        { timeout: 600000 }
      );
      console.log('[spark-engine] Podman installation completed');

      // Re-check if Podman is now available (refresh the module-level bin path)
      try {
        PODMAN_BIN = resolvePodmanBin();
        await exec(`${PODMAN_BIN} --version`, { env: this.getPodmanEnv() });
        console.log('[spark-engine] Podman verified after installation');
        writeFileSync(markerPath, `installed=${new Date().toISOString()}\n`);
        return true;
      } catch {
        console.error('[spark-engine] Podman still not detected after installation');
        writeFileSync(markerPath, `failed=${new Date().toISOString()}\n`);
        return false;
      }
    } catch (err) {
      console.error('[spark-engine] Podman auto-install failed:', err instanceof Error ? err.message : String(err));
      writeFileSync(markerPath, `failed=${new Date().toISOString()}\nreason=${err instanceof Error ? err.message : String(err)}\n`);
      return false;
    }
  }

  /**
   * Ensure the Spark container is running and healthy.
   * Follows a priority chain to minimize startup time:
   *   1. Container running + healthy → reuse instantly
   *   2. Container stopped → start it, wait for health
   *   3. Image exists, no container → run new container
   *   4. No image → build image, then run container
   * Returns true if container is ready, false if unavailable.
   */
  private async ensureContainer(): Promise<boolean> {
    const env = this.getPodmanEnv();

    // Step 1 & 2: Check existing container state
    let containerExists = false;
    try {
      const { stdout } = await exec(
        `${PODMAN_BIN} inspect --format {{.State.Status}} ${this.containerName}`,
        { env, timeout: 10000 }
      );
      const state = stdout.trim();
      containerExists = true;

      if (state === 'running') {
        // Step 1: Running — quick health check (10s). If Spark crashed inside
        // a long-running container, don't waste 120s polling a zombie.
        this.setProgress('Waiting for compute');
        console.log(`[api-local] Found running Spark container, checking health...`);
        const quickReady = await this.pollHealthQuick(10000);
        if (quickReady) return true;
        // Stale container — remove and fall through to recreate
        console.log('[api-local] Existing container unhealthy, recreating...');
        await exec(`${PODMAN_BIN} rm -f ${this.containerName}`, { env, timeout: 10000 }).catch(() => {});
        containerExists = false;
      } else {
        // Step 2: Stopped/exited — start it
        this.setProgress('Starting compute');
        console.log(`[api-local] Starting stopped Spark container...`);
        await exec(`${PODMAN_BIN} start ${this.containerName}`, { env, timeout: 30000 });
        this.setProgress('Waiting for compute');
        return await this.pollHealth();
      }
    } catch {
      // Container doesn't exist
    }

    // Step 3: Check if image exists (skip build if so)
    let imageExists = false;
    try {
      await exec(`${PODMAN_BIN} image exists ${SPARK_IMAGE}`, { env, timeout: 10000 });
      imageExists = true;
    } catch {
      // Image doesn't exist
    }

    // Step 4: Build image if needed
    if (!imageExists) {
      const sparkDir = process.env.SPARK_CONTEXT_DIR || join(__dirname, '..', 'spark');
      const normalizedSparkDir = sparkDir.replace(/\\/g, '/');
      try {
        this.setProgress('Building image');
        console.log(`[api-local] Building Spark Connect image...`);
        await this.runPodmanBuild(normalizedSparkDir, env);
        console.log(`[api-local] Spark Connect image built successfully.`);
      } catch (buildErr) {
        this.currentStatus = "unavailable";
        this.unavailableReason = `Failed to build Spark image: ${buildErr instanceof Error ? buildErr.message : String(buildErr)}`;
        return false;
      }
    }

    // Clean up stale container if it exists but wasn't running
    if (containerExists) {
      await exec(`${PODMAN_BIN} rm -f ${this.containerName}`, { env, timeout: 10000 }).catch(() => {});
    }

    // Run new container
    const resultsDir = join(this.dataDir, 'operations', 'results');
    mkdirSync(resultsDir, { recursive: true });

    const normalizedWarehouseDir = this.warehouseDir.replace(/\\/g, '/');
    const normalizedResultsDir = resultsDir.replace(/\\/g, '/');

    // Build environment flags for the container
    const envFlags = [
      `-e WAREHOUSE_PATH=/warehouse`,
      `-e HTTP_PORT=${this.sparkPort}`,
      `-e SPARK_DRIVER_MEMORY=${this.driverMemoryGb}g`,
      `-e PYTHONUNBUFFERED=1`,
      // Cloud credential passthrough (only set if available on the host)
      ...this.buildCloudCredentialFlags(),
    ];

    // Use explicit port mapping instead of --network host.
    // On Windows/macOS, Podman runs in a WSL2/HyperV VM and --network host
    // maps to the VM's network, not the Windows host — ports are unreachable.
    const runCmd = [
      `${PODMAN_BIN} run -d`,
      `--name ${this.containerName}`,
      `-p ${this.sparkPort}:${this.sparkPort}`,
      `--user root`,
      `-v "${normalizedWarehouseDir}":/warehouse`,
      `-v "${normalizedResultsDir}":/results`,
      ...envFlags,
      SPARK_IMAGE,
    ].join(' ');

    try {
      await exec(runCmd, { env });
      this.setProgress('Waiting for compute');
      console.log(`[api-local] Spark container started, polling health on port ${this.sparkPort}...`);
      return await this.pollHealth();
    } catch (runErr) {
      this.currentStatus = "unavailable";
      this.unavailableReason = `Failed to start Spark container: ${runErr instanceof Error ? runErr.message : String(runErr)}`;
      return false;
    }
  }

  /**
   * Build -e flags for cloud storage credentials available on the host.
   * Only passes credentials that are actually set -- Spark ignores unconfigured providers.
   */
  private buildCloudCredentialFlags(): string[] {
    const flags: string[] = [];
    const pass = (key: string) => {
      const value = process.env[key];
      if (value) flags.push(`-e ${key}=${value}`);
    };
    // AWS: credentials are resolved on-demand per S3/Glue import via
    // configureSparkS3Credentials() -- no container-level env vars needed.
    pass('AWS_REGION');
    pass('AWS_DEFAULT_REGION');
    // GCS
    pass('GOOGLE_APPLICATION_CREDENTIALS');
    pass('GOOGLE_CLOUD_PROJECT');
    // Azure
    pass('AZURE_STORAGE_ACCOUNT');
    pass('AZURE_STORAGE_KEY');
    pass('AZURE_STORAGE_CONNECTION_STRING');
    return flags;
  }

  /**
   * Check if the Spark sidecar HTTP endpoint is healthy.
   */
  private async checkSidecarHealth(): Promise<boolean> {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 5000);
      const response = await fetch(`http://localhost:${this.sparkPort}/health`, {
        signal: controller.signal,
      });
      clearTimeout(timeout);
      if (response.ok) {
        const body = await response.json() as { status: string; version?: string };
        return body.status === 'ready';
      }
    } catch {
      // Not responding or timed out
    }
    return false;
  }

  /**
   * Quick health check with a short timeout. Used to detect stale containers
   * that are running but have a crashed Spark process inside.
   */
  private async pollHealthQuick(timeoutMs: number): Promise<boolean> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (await this.checkSidecarHealth()) {
        this.currentStatus = "ready";
        console.log(`[api-local] Spark sidecar ready on port ${this.sparkPort}`);
        return true;
      }
      await new Promise(r => setTimeout(r, HEALTH_POLL_INTERVAL_MS));
    }
    return false;
  }

  /**
   * Run podman build with streaming output, parsing STEP X/Y for progress.
   */
  private runPodmanBuild(contextDir: string, env: Record<string, string>): Promise<void> {
    return new Promise((resolve, reject) => {
      // PODMAN_BIN may be wrapped in quotes (e.g. "C:\Program Files\...\podman.exe")
      // spawn() handles spaces natively — strip quotes and pass the raw path
      const bin = PODMAN_BIN.replace(/^"|"$/g, '');
      const proc = nodeSpawn(bin, ['build', '-t', SPARK_IMAGE, contextDir], { env });

      const timer = setTimeout(() => {
        proc.kill();
        reject(new Error('Image build timed out after 20 minutes'));
      }, 1200000);

      const parseStep = (data: Buffer): void => {
        const match = data.toString().match(/STEP (\d+)\/(\d+)/);
        if (match) {
          this.setProgress(`Building image (step ${match[1]} of ${match[2]})`);
        }
      };

      proc.stdout?.on('data', parseStep);
      proc.stderr?.on('data', parseStep);

      proc.on('close', (code) => {
        clearTimeout(timer);
        if (code === 0) resolve();
        else reject(new Error(`podman build exited with code ${code}`));
      });
      proc.on('error', (err) => {
        clearTimeout(timer);
        reject(err);
      });
    });
  }

  /**
   * Poll the sidecar health endpoint until ready or timeout.
   * Sets currentStatus to "ready" on success, "unavailable" on timeout.
   */
  private async pollHealth(): Promise<boolean> {
    const startTime = Date.now();
    while (Date.now() - startTime < HEALTH_TIMEOUT_MS) {
      if (await this.checkSidecarHealth()) {
        this.currentStatus = "ready";
        console.log(`[api-local] Spark sidecar ready on port ${this.sparkPort}`);
        return true;
      }
      await new Promise(resolve => setTimeout(resolve, HEALTH_POLL_INTERVAL_MS));
    }
    this.currentStatus = "unavailable";
    this.unavailableReason = `Spark sidecar did not become ready within ${HEALTH_TIMEOUT_MS / 1000}s`;
    return false;
  }

  /**
   * Attempt to restart the container when the sidecar becomes unresponsive
   * (e.g. Spark Connect JVM crashed, leaving a zombie process).
   * Returns true if recovery succeeded and sidecar is healthy again.
   */
  private async attemptRecovery(): Promise<boolean> {
    console.log('[spark-engine] Sidecar unresponsive, attempting container restart...');
    this.currentStatus = "initializing";
    const env = this.getPodmanEnv();
    try {
      await exec(`${PODMAN_BIN} restart ${this.containerName}`, { env, timeout: 60000 });
      const healthy = await this.pollHealth();
      if (healthy) {
        console.log('[spark-engine] Recovery successful, sidecar is healthy again.');
        return true;
      }
    } catch (err) {
      console.error('[spark-engine] Recovery failed:', err instanceof Error ? err.message : String(err));
    }
    this.currentStatus = "unavailable";
    this.unavailableReason = 'Spark Connect crashed and recovery failed. Restart the app.';
    return false;
  }

  /**
   * Execute a SQL query and return results.
   * Handles both DDL (returns empty result) and SELECT queries.
   */
  async executeSQL(sql: string, _database?: string, jobId?: string, resultSetLimit?: number): Promise<QueryResult> {
    if (this.currentStatus !== "ready") {
      throw new Error(`Spark engine is not ready (status: ${this.currentStatus})`);
    }

    const payload: Record<string, unknown> = { sql, jobId };
    if (resultSetLimit != null && resultSetLimit > 0) payload.limit = resultSetLimit;

    let response: Response;
    try {
      response = await fetch(`http://localhost:${this.sparkPort}/execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(SPARK_QUERY_TIMEOUT_MS),
      });
    } catch (fetchErr) {
      // Connection refused / timeout — Spark Connect may have crashed
      const recovered = await this.attemptRecovery();
      if (!recovered) throw new Error(`Spark engine crashed and could not recover: ${fetchErr instanceof Error ? fetchErr.message : String(fetchErr)}`);
      // Retry once after recovery
      response = await fetch(`http://localhost:${this.sparkPort}/execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(SPARK_QUERY_TIMEOUT_MS),
      });
    }

    if (!response.ok) {
      let errorMessage = response.statusText;
      try {
        const errorBody = await response.json() as SparkExecuteResponse;
        errorMessage = errorBody.error ?? response.statusText;
      } catch {
        // Sidecar returned non-JSON (e.g. raw HTTP error text)
        errorMessage = await response.text().catch(() => response.statusText);
      }
      throw new Error(`Spark execution failed: ${errorMessage}`);
    }

    const result = await response.json() as SparkExecuteResponse;
    return {
      columns: result.columns,
      rows: result.rows,
      rowCount: result.rowCount,
    };
  }

  /**
   * Execute PySpark/Python code.
   * The sidecar provides `spark` (SparkSession) in scope.
   * If the code assigns to `result` or `_result`, that DataFrame is returned.
   */
  async executePython(code: string, jobId?: string, resultSetLimit?: number): Promise<QueryResult> {
    if (this.currentStatus !== "ready") {
      throw new Error(`Spark engine is not ready (status: ${this.currentStatus})`);
    }

    const payload: Record<string, unknown> = { code, jobId };
    if (resultSetLimit != null && resultSetLimit > 0) payload.limit = resultSetLimit;

    let response: Response;
    try {
      response = await fetch(`http://localhost:${this.sparkPort}/execute-python`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(SPARK_QUERY_TIMEOUT_MS),
      });
    } catch (fetchErr) {
      const recovered = await this.attemptRecovery();
      if (!recovered) throw new Error(`Spark engine crashed and could not recover: ${fetchErr instanceof Error ? fetchErr.message : String(fetchErr)}`);
      response = await fetch(`http://localhost:${this.sparkPort}/execute-python`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(SPARK_QUERY_TIMEOUT_MS),
      });
    }

    if (!response.ok) {
      let errorMessage = response.statusText;
      try {
        const errorBody = await response.json() as SparkExecuteResponse;
        errorMessage = errorBody.error ?? response.statusText;
      } catch {
        errorMessage = await response.text().catch(() => response.statusText);
      }
      throw new Error(`PySpark execution failed: ${errorMessage}`);
    }

    const result = await response.json() as SparkExecuteResponse;
    return {
      columns: result.columns,
      rows: result.rows,
      rowCount: result.rowCount,
    };
  }

  /**
   * Execute a DDL statement (CREATE, DROP, ALTER, INSERT, etc.).
   * Does not return data -- throws on failure.
   */
  async executeDDL(sql: string): Promise<void> {
    if (this.currentStatus !== "ready") {
      throw new Error(`Spark engine is not ready (status: ${this.currentStatus})`);
    }

    let response: Response;
    try {
      response = await fetch(`http://localhost:${this.sparkPort}/execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql }),
        signal: AbortSignal.timeout(SPARK_QUERY_TIMEOUT_MS),
      });
    } catch (fetchErr) {
      const recovered = await this.attemptRecovery();
      if (!recovered) throw new Error(`Spark engine crashed and could not recover: ${fetchErr instanceof Error ? fetchErr.message : String(fetchErr)}`);
      response = await fetch(`http://localhost:${this.sparkPort}/execute`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql }),
        signal: AbortSignal.timeout(SPARK_QUERY_TIMEOUT_MS),
      });
    }

    if (!response.ok) {
      let errorMessage = response.statusText;
      try {
        const errorBody = await response.json() as SparkExecuteResponse;
        errorMessage = errorBody.error ?? response.statusText;
      } catch {
        errorMessage = await response.text().catch(() => response.statusText);
      }
      throw new Error(`Spark DDL failed: ${errorMessage}`);
    }
  }

  /**
   * Get the warehouse filesystem path for a given namespace.table.
   * Returns the host-side path (mapped from container /warehouse).
   */
  async getTablePath(namespace: string, table: string): Promise<string> {
    if (this.currentStatus !== "ready") {
      throw new Error(`Spark engine is not ready (status: ${this.currentStatus})`);
    }

    const url = new URL(`http://localhost:${this.sparkPort}/table-path`);
    url.searchParams.set('namespace', namespace);
    url.searchParams.set('table', table);

    const response = await fetch(url.toString());
    if (!response.ok) {
      const errorBody = await response.json() as SparkTablePathResponse;
      throw new Error(`Failed to get table path: ${errorBody.error ?? response.statusText}`);
    }

    const result = await response.json() as SparkTablePathResponse;
    // The container returns /warehouse/ns/table -- map to host warehouse dir
    const containerPath = result.path;
    const relativePath = containerPath.replace(/^\/warehouse\/?/, '');
    return join(this.warehouseDir, relativePath);
  }

  /** Returns the host-side warehouse directory path */
  getWarehouseDir(): string {
    return this.warehouseDir;
  }

  async cancel(jobId: string): Promise<void> {
    if (this.currentStatus !== "ready") return;

    try {
      await fetch(`http://localhost:${this.sparkPort}/stop`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jobId }),
      });
    } catch {
      // Best-effort cancellation
    }
  }

  async recycle(): Promise<void> {
    const env = this.getPodmanEnv();

    try {
      await exec(`${PODMAN_BIN} rm -f ${this.containerName}`, { env, timeout: 15000 }).catch(() => {});
      this.currentStatus = "initializing";
      this.unavailableReason = undefined;
      this.progressMessage = undefined;
      await this.ensureContainer();
    } catch (error) {
      this.currentStatus = "unavailable";
      this.unavailableReason = error instanceof Error ? error.message : String(error);
      throw error;
    }
  }

  /** Mark engine as disabled (spark not enabled in config). */
  disable(reason: string): void {
    this.currentStatus = "unavailable";
    this.unavailableReason = reason;
  }

  status(): SparkStatus {
    return this.currentStatus;
  }

  getUnavailableReason(): string | undefined {
    return this.unavailableReason;
  }

  getProgressMessage(): string | undefined {
    return this.progressMessage;
  }

  async shutdown(): Promise<void> {
    const env = this.getPodmanEnv();
    try {
      await exec(`${PODMAN_BIN} stop ${this.containerName}`, { env });
      await exec(`${PODMAN_BIN} rm ${this.containerName}`, { env });
    } catch {
      // Container may already be stopped
    }

    // Stop the Podman machine (Linux VM) to free ~2GB RAM.
    // The VM will be restarted automatically on next launch.
    if (this.managePodmanMachineOnShutdown && (process.platform === 'win32' || process.platform === 'darwin')) {
      try {
        console.log('[spark-engine] Stopping Podman machine...');
        await exec(`${PODMAN_BIN} machine stop`, { env, timeout: 30000 });
        console.log('[spark-engine] Podman machine stopped');
      } catch {
        // Machine may already be stopped or not initialized
      }
    }
  }
}

/** Background pre-warm -- called from server.ts, does not block startup */
export async function prewarmSpark(engine: SparkEngine): Promise<void> {
  try {
    await engine.init();
  } catch (err) {
    // Non-fatal -- Spark unavailable
    console.warn('[api-local] Spark engine unavailable:', err);
  }
}
