import { readFileSync } from 'fs';
import { homedir } from 'os';
import { join } from 'path';
import toml from 'toml';

export interface LocalConfig {
  ai: {
    mode: "claude-api" | "claude-bedrock" | "web-agent";
    provider: string;
    model: string;
    localModel?: string;
    localPort?: number;
    apiKey?: string;
  };
  local: {
    dataDir: string;
    maxResultRows: number;
    queryTimeoutWarnMs: number;
    /** Team name — used as the default database name in local catalog */
    teamName: string;
  };
  compute: {
    defaultEngine: "spark";
    sparkEnabled: boolean;
    sparkPort: number;
    sparkPrewarm: boolean;
    maxConcurrentQueries: number;
    /** Override auto-scaled Spark driver memory (in GB). When absent, auto-scaling applies. */
    sparkDriverMemoryGb?: number;
  };
}

interface TomlParsed {
  ai?: {
    mode?: string;
    provider?: string;
    model?: string;
    local_model?: string;
    local_port?: number;
    api_key?: string;
  };
  local?: {
    data_dir?: string;
    max_result_rows?: number;
    query_timeout_warn_ms?: number;
    team_name?: string;
  };
  compute?: {
    default_engine?: string;
    spark_enabled?: boolean;
    spark_port?: number;
    spark_prewarm?: boolean;
    max_concurrent_queries?: number;
    spark_driver_memory_gb?: number;
  };
}

export function loadConfig(): LocalConfig {
  // When PODMAN_INSTALLER_PATH is set, we're running inside the bundled desktop
  // app — auto-enable Spark so Podman auto-install kicks in on first launch.
  const isBundledDesktop = !!process.env['PODMAN_BIN_DIR'] || !!process.env['PODMAN_INSTALLER_PATH'];

  const configPath = join(homedir(), '.nile', 'config.toml');
  let raw: string;
  try {
    raw = readFileSync(configPath, 'utf-8');
  } catch {
    // First run -- return defaults; config.toml created by M5 setup wizard
    return {
      ai: { mode: "web-agent", provider: "ollama", model: "qwen3-coder", apiKey: undefined },
      local: {
        dataDir: join(homedir(), '.nile'),
        maxResultRows: 10000,
        queryTimeoutWarnMs: 300000,
        teamName: "local",
      },
      compute: {
        defaultEngine: "spark",
        sparkEnabled: isBundledDesktop,
        sparkPort: 3002,
        sparkPrewarm: false,
        maxConcurrentQueries: 2,
      },
    };
  }

  const parsed: TomlParsed = toml.parse(raw) as TomlParsed;

  return {
    ai: {
      mode: (parsed.ai?.mode as LocalConfig["ai"]["mode"]) ?? "web-agent",
      provider: parsed.ai?.provider ?? "ollama",
      model: parsed.ai?.local_model ?? parsed.ai?.model ?? "qwen3-coder",
      localModel: parsed.ai?.local_model ?? parsed.ai?.model,
      localPort: parsed.ai?.local_port,
      apiKey: parsed.ai?.api_key,
    },
    local: {
      dataDir: parsed.local?.data_dir ?? join(homedir(), '.nile'),
      maxResultRows: parsed.local?.max_result_rows ?? 10000,
      queryTimeoutWarnMs: parsed.local?.query_timeout_warn_ms ?? 300000,
      teamName: parsed.local?.team_name ?? "local",
    },
    compute: {
      defaultEngine: (parsed.compute?.default_engine as "spark") ?? "spark",
      sparkEnabled: parsed.compute?.spark_enabled ?? isBundledDesktop,
      sparkPort: parsed.compute?.spark_port ?? 3002,
      sparkPrewarm: parsed.compute?.spark_prewarm ?? false,
      maxConcurrentQueries: Math.max(1, parsed.compute?.max_concurrent_queries ?? 2),
      sparkDriverMemoryGb: parsed.compute?.spark_driver_memory_gb,
    },
  };
}
