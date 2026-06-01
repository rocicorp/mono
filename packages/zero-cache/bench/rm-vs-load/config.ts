import {FORWARDER_FLOW_CONTROL_BYTES_THRESHOLD} from '../../src/services/change-streamer/change-streamer-service.ts';
import {PROTOCOL_VERSION} from '../../src/services/change-streamer/change-streamer.ts';
import {SERVING_REPLICA_WAL_AUTOCHECKPOINT_PAGES} from '../../src/workers/replicator.ts';
import type {
  BenchmarkConfig,
  ConsumerApplyMode,
  ConsumerConfig,
  ConsumerProtocolMode,
  ConsumerRuntime,
  ConsumerTransportAckMode,
  ConsumerTransportMode,
} from './types.ts';

export class EnvReader {
  readonly #env: Record<string, string | undefined>;
  readonly #argv: readonly string[];

  constructor(
    env: Record<string, string | undefined> = process.env,
    argv: readonly string[] = process.argv,
  ) {
    this.#env = env;
    this.#argv = argv;
  }

  string(name: string, fallback?: string): string | undefined {
    const value = this.#env[name];
    return value === undefined || value === '' ? fallback : value;
  }

  int(name: string, fallback: number): number {
    const value = this.string(name);
    if (value === undefined) {
      return fallback;
    }
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed)) {
      throw new Error(`Invalid integer ${name}=${value}`);
    }
    return parsed;
  }

  optionalInt(name: string, defaultValue?: number): number | undefined {
    const value = this.string(name);
    if (value === undefined) {
      return defaultValue;
    }
    return this.int(name, 0);
  }

  flag(name: string): boolean {
    const value = this.string(name);
    return value === '1' || value === 'true' || value === 'yes';
  }

  argValue(name: string): string | undefined {
    const long = `--${name}`;
    const withEquals = `${long}=`;
    for (let i = 2; i < this.#argv.length; i++) {
      const arg = this.#argv[i];
      if (arg === long) {
        return this.#argv[i + 1];
      }
      if (arg?.startsWith(withEquals)) {
        return arg.slice(withEquals.length);
      }
    }
    return undefined;
  }
}

export class BenchmarkConfigLoader {
  readonly #env: EnvReader;

  constructor(env = new EnvReader()) {
    this.#env = env;
  }

  load(): BenchmarkConfig {
    const full = this.#env.flag('ZERO_RM_VS_FULL');
    const applyMode = this.#applyMode();
    const consumer = this.#consumerConfig(full, applyMode);
    return {
      mode: full ? 'full' : 'smoke',
      full,
      durationMs: this.#env.int('ZERO_RM_VS_DURATION_MS', full ? 2_500 : 1_000),
      settleMs: this.#env.int('ZERO_RM_VS_SETTLE_MS', 100),
      flushBytesThreshold: this.#env.int(
        'ZERO_RM_VS_FLUSH_BYTES',
        FORWARDER_FLOW_CONTROL_BYTES_THRESHOLD,
      ),
      reconnectLagTx: this.#env.int('ZERO_RM_VS_RECONNECT_LAG_TX', 64),
      reconnectFinalCatchupTimeoutMs: this.#env.int(
        'ZERO_RM_VS_FINAL_CATCHUP_TIMEOUT_MS',
        full ? 15_000 : 5_000,
      ),
      sourceApply: this.#env.flag('ZERO_RM_VS_SOURCE_APPLY'),
      workerBatchMessages: this.#env.int(
        'ZERO_RM_VS_WORKER_BATCH_MESSAGES',
        64,
      ),
      pgImage: this.#env.string('ZERO_RM_VS_PG_IMAGE') ?? 'postgres:17',
      outputPath:
        this.#env.argValue('out') ?? this.#env.string('ZERO_RM_VS_OUT'),
      consumer,
    };
  }

  #consumerConfig(full: boolean, applyMode: ConsumerApplyMode): ConsumerConfig {
    return {
      count: this.#env.int('ZERO_RM_VS_SUBSCRIBERS', full ? 16 : 4),
      ackDelayMs: this.#env.int('ZERO_RM_VS_ACK_DELAY_MS', 0),
      runtime: this.#runtime(),
      applyMode,
      applyMessages: applyMode !== 'none',
      applyLimit: this.#env.optionalInt('ZERO_RM_VS_APPLY_LIMIT'),
      transportMode: this.#transportMode(),
      transportAckMode: this.#transportAckMode(),
      transportBatchMessages: this.#env.int('ZERO_RM_VS_WS_BATCH_MESSAGES', 64),
      protocolMode: this.#protocolMode(),
      synchronous: this.#synchronous(),
      walAutocheckpoint: this.#env.optionalInt(
        'ZERO_RM_VS_WAL_AUTOCHECKPOINT',
        SERVING_REPLICA_WAL_AUTOCHECKPOINT_PAGES,
      ),
      clientCpuMicros: this.#env.int('ZERO_RM_VS_CLIENT_CPU_US', 0),
      slowAckDelayMs: this.#env.int(
        'ZERO_RM_VS_SLOW_ACK_DELAY_MS',
        full ? 2 : 1,
      ),
      slowEvery: this.#env.int('ZERO_RM_VS_SLOW_EVERY', full ? 4 : 2),
    };
  }

  #applyMode(): ConsumerApplyMode {
    const mode = this.#env.string('ZERO_RM_VS_APPLY_MODE');
    if (mode === undefined) {
      return this.#env.flag('ZERO_RM_VS_APPLY_CLIENTS') ? 'direct' : 'none';
    }
    switch (mode) {
      case 'none':
      case 'direct':
      case 'worker-message':
      case 'worker-batch':
        return mode;
      default:
        throw new Error(
          `Invalid ZERO_RM_VS_APPLY_MODE=${mode}; expected ` +
            'none, direct, worker-message, or worker-batch',
        );
    }
  }

  #runtime(): ConsumerRuntime {
    const runtime = this.#env.string('ZERO_RM_VS_CONSUMER_RUNTIME') ?? 'inline';
    switch (runtime) {
      case 'inline':
      case 'worker':
        return runtime;
      default:
        throw new Error(
          `Invalid ZERO_RM_VS_CONSUMER_RUNTIME=${runtime}; expected ` +
            'inline or worker',
        );
    }
  }

  #synchronous(): 'OFF' | 'NORMAL' | 'FULL' | undefined {
    const value = this.#env.string('ZERO_RM_VS_SQLITE_SYNCHRONOUS');
    switch (value) {
      case undefined:
      case '':
        return undefined;
      case 'OFF':
      case 'NORMAL':
      case 'FULL':
        return value;
      default:
        throw new Error(`Invalid ZERO_RM_VS_SQLITE_SYNCHRONOUS ${value}`);
    }
  }

  #transportAckMode(): ConsumerTransportAckMode {
    const mode = this.#env.string('ZERO_RM_VS_WS_ACK');
    if (mode === undefined) {
      return 'per-message';
    }
    switch (mode) {
      case 'per-message':
      case 'cumulative':
        return mode;
      default:
        throw new Error(
          `Invalid ZERO_RM_VS_WS_ACK=${mode}; expected per-message or cumulative`,
        );
    }
  }

  #transportMode(): ConsumerTransportMode {
    const mode = this.#env.string('ZERO_RM_VS_TRANSPORT');
    if (mode === undefined) {
      return 'in-process';
    }
    switch (mode) {
      case 'in-process':
      case 'websocket':
        return mode;
      default:
        throw new Error(
          `Invalid ZERO_RM_VS_TRANSPORT=${mode}; expected in-process or websocket`,
        );
    }
  }

  #protocolMode(): ConsumerProtocolMode {
    const mode = this.#env.string('ZERO_RM_VS_PROTOCOL') ?? 'v6';
    switch (mode) {
      case 'v6':
        return mode;
      default:
        throw new Error(`Invalid ZERO_RM_VS_PROTOCOL=${mode}; expected v6`);
    }
  }
}

export function protocolVersionForMode(_mode: ConsumerProtocolMode): number {
  return PROTOCOL_VERSION;
}
