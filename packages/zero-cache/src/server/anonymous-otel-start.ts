import {type Meter} from '@opentelemetry/api';
import {OTLPMetricExporter} from '@opentelemetry/exporter-metrics-otlp-http';
import {PeriodicExportingMetricReader} from '@opentelemetry/sdk-metrics';
import {MeterProvider} from '@opentelemetry/sdk-metrics';
import {resourceFromAttributes} from '@opentelemetry/resources';
import type {ObservableResult} from '@opentelemetry/api';
import {platform} from 'os';
import {h64} from '../../../shared/src/hash.js';
import type {LogContext} from '@rocicorp/logger';
import packageJson from '../../../zero/package.json' with {type: 'json'};
import {getZeroConfig, type ZeroConfig} from '../config/zero-config.js';
import {execSync} from 'child_process';
import {randomUUID} from 'crypto';
import {existsSync, readFileSync, writeFileSync, mkdirSync} from 'fs';
import {join, dirname} from 'path';
import {homedir} from 'os';

class AnonymousTelemetryManager {
  static #instance: AnonymousTelemetryManager;
  #starting = false;
  #stopped = false;
  #meter!: Meter;
  #meterProvider!: MeterProvider;
  #totalMutations = 0;
  #totalRowsSynced = 0;
  #totalConnectionsSuccess = 0;
  #totalConnectionsAttempted = 0;
  #lc: LogContext | undefined;
  #config: ZeroConfig | undefined;
  #processId: string;
  #cachedAttributes: Record<string, string> | undefined;
  #viewSyncerCount = 1;

  private constructor() {
    this.#processId = randomUUID();
  }

  static getInstance(): AnonymousTelemetryManager {
    if (!AnonymousTelemetryManager.#instance) {
      AnonymousTelemetryManager.#instance = new AnonymousTelemetryManager();
    }
    return AnonymousTelemetryManager.#instance;
  }

  start(lc?: LogContext, config?: ZeroConfig) {
    if (!config) {
      try {
        config = getZeroConfig();
      } catch (e) {
        this.#lc?.debug?.(
          'Anonymous telemetry disabled: unable to parse config',
          e,
        );
        return;
      }
    }

    if (process.env.DO_NOT_TRACK) {
      this.#lc?.debug?.(
        'Anonymous telemetry disabled: DO_NOT_TRACK environment variable is set',
      );
      return;
    }

    if (this.#starting || !config.enableUsageAnalytics) {
      return;
    }

    this.#starting = true;
    this.#lc = lc;
    this.#config = config;
    this.#viewSyncerCount = config.numSyncWorkers ?? 1;
    this.#cachedAttributes = undefined;

    this.#lc?.info?.(`Anonymous telemetry will start in 1 minute`);

    // Delay telemetry startup by 1 minute to avoid potential boot loop issues
    setTimeout(() => this.#run(), 60000);
  }

  #run() {
    if (this.#stopped) {
      return;
    }

    const resource = resourceFromAttributes(this.#getAttributes());

    const metricReader = new PeriodicExportingMetricReader({
      exportIntervalMillis: 60000 * this.#viewSyncerCount,
      exporter: new OTLPMetricExporter({
        url: 'https://metrics.rocicorp.dev',
      }),
    });

    this.#meterProvider = new MeterProvider({
      resource,
      readers: [metricReader],
    });
    this.#meter = this.#meterProvider.getMeter('zero-anonymous-telemetry');

    this.#setupMetrics();
    this.#lc?.info?.(
      `Anonymous telemetry started (exports every ${60 * this.#viewSyncerCount} seconds, scaled by ${this.#viewSyncerCount} view-syncers)`,
    );
  }

  #setupMetrics() {
    // Observable gauges
    const uptimeGauge = this.#meter.createObservableGauge('zero.uptime', {
      description: 'System uptime in seconds',
      unit: 'seconds',
    });

    // Observable counters
    const uptimeCounter = this.#meter.createObservableCounter(
      'zero.uptime_counter',
      {
        description: 'System uptime in seconds',
        unit: 'seconds',
      },
    );
    const mutationsCounter = this.#meter.createObservableCounter(
      'zero.mutations_processed',
      {
        description: 'Total number of mutations processed',
      },
    );
    const rowsSyncedCounter = this.#meter.createObservableCounter(
      'zero.rows_synced',
      {
        description: 'Total number of rows synced',
      },
    );

    // Observable counters for connections
    const connectionsSuccessCounter = this.#meter.createObservableCounter(
      'zero.connections_success',
      {
        description: 'Total number of successful connections',
      },
    );

    const connectionsAttemptedCounter = this.#meter.createObservableCounter(
      'zero.connections_attempted',
      {
        description: 'Total number of attempted connections',
      },
    );

    // Callbacks
    const attrs = this.#getAttributes();
    uptimeGauge.addCallback((result: ObservableResult) => {
      const uptimeSeconds = Math.floor(process.uptime());
      result.observe(uptimeSeconds, attrs);
      this.#lc?.debug?.(`Telemetry: uptime=${uptimeSeconds}s`);
    });
    uptimeCounter.addCallback((result: ObservableResult) => {
      const uptimeSeconds = Math.floor(process.uptime());
      result.observe(uptimeSeconds, attrs);
      this.#lc?.debug?.(`Telemetry: uptime_counter=${uptimeSeconds}s`);
    });
    mutationsCounter.addCallback((result: ObservableResult) => {
      result.observe(this.#totalMutations, attrs);
      this.#lc?.debug?.(
        `Telemetry: mutations_processed=${this.#totalMutations}`,
      );
    });
    rowsSyncedCounter.addCallback((result: ObservableResult) => {
      result.observe(this.#totalRowsSynced, attrs);
      this.#lc?.debug?.(`Telemetry: rows_synced=${this.#totalRowsSynced}`);
    });
    connectionsSuccessCounter.addCallback((result: ObservableResult) => {
      result.observe(this.#totalConnectionsSuccess, attrs);
      this.#lc?.debug?.(
        `Telemetry: connections_success=${this.#totalConnectionsSuccess}`,
      );
    });
    connectionsAttemptedCounter.addCallback((result: ObservableResult) => {
      result.observe(this.#totalConnectionsAttempted, attrs);
      this.#lc?.debug?.(
        `Telemetry: connections_attempted=${this.#totalConnectionsAttempted}`,
      );
    });
  }

  recordMutation(count = 1) {
    this.#totalMutations += count;
  }

  recordRowsSynced(count: number) {
    this.#totalRowsSynced += count;
  }

  recordConnectionSuccess() {
    this.#totalConnectionsSuccess++;
  }

  recordConnectionAttempted() {
    this.#totalConnectionsAttempted++;
  }

  shutdown() {
    this.#stopped = true;
    if (this.#meterProvider) {
      this.#lc?.info?.('Shutting down anonymous telemetry');
      void this.#meterProvider.shutdown();
    }
  }

  #getAttributes() {
    if (!this.#cachedAttributes) {
      this.#cachedAttributes = {
        'zero.app.id': h64(this.#config?.upstream.db || 'unknown').toString(),
        'zero.machine.os': platform(),
        'zero.telemetry.type': 'anonymous',
        'zero.infra.platform': this.#getPlatform(),
        'zero.version': this.#config?.serverVersion ?? packageJson.version,
        'zero.task.id': this.#config?.taskID || 'unknown',
        'zero.project.id': this.#getGitProjectId(),
        'zero.process.id': this.#processId,
        'zero.fs.id': this.#getOrSetFsID(),
      };
      this.#lc?.debug?.(
        `Telemetry: cached attributes=${JSON.stringify(this.#cachedAttributes)}`,
      );
    }
    return this.#cachedAttributes;
  }

  #getPlatform(): string {
    if (process.env.FLY_APP_NAME || process.env.FLY_REGION) return 'fly.io';
    if (
      process.env.ECS_CONTAINER_METADATA_URI_V4 ||
      process.env.ECS_CONTAINER_METADATA_URI ||
      process.env.AWS_REGION ||
      process.env.AWS_EXECUTION_ENV
    )
      return 'aws';
    if (process.env.RAILWAY_ENV || process.env.RAILWAY_STATIC_URL)
      return 'railway';
    if (process.env.RENDER || process.env.RENDER_SERVICE_ID) return 'render';
    if (
      process.env.GCP_PROJECT ||
      process.env.GCLOUD_PROJECT ||
      process.env.GOOGLE_CLOUD_PROJECT
    )
      return 'gcp';
    if (process.env.COOLIFY_URL || process.env.COOLIFY_CONTAINER_NAME)
      return 'coolify';

    return 'unknown';
  }

  #findUp(startDir: string, target: string): string | null {
    let dir = startDir;
    while (dir !== dirname(dir)) {
      if (existsSync(join(dir, target))) return dir;
      dir = dirname(dir);
    }
    return null;
  }

  #getGitProjectId(): string {
    try {
      const cwd = process.cwd();
      const gitRoot = this.#findUp(cwd, '.git');
      if (!gitRoot) {
        return 'unknown';
      }

      const rootCommitHash = execSync('git rev-list --max-parents=0 HEAD -1', {
        cwd: gitRoot,
        encoding: 'utf8',
        timeout: 1000,
        stdio: ['ignore', 'pipe', 'ignore'], // Suppress stderr
      }).trim();

      return rootCommitHash.length === 40 ? rootCommitHash : 'unknown';
    } catch (error) {
      this.#lc?.debug?.('Unable to get Git root commit:', error);
      return 'unknown';
    }
  }

  #getOrSetFsID(): string {
    try {
      if (this.#isInContainer()) {
        return 'container';
      }
      const fsidPath = join(homedir(), '.rocicorp', 'fsid');
      const fsidDir = dirname(fsidPath);

      if (existsSync(fsidPath)) {
        const existingId = readFileSync(fsidPath, 'utf8').trim();
        return existingId;
      }

      if (!existsSync(fsidDir)) {
        mkdirSync(fsidDir, {recursive: true});
      }

      const newId = randomUUID();
      writeFileSync(fsidPath, newId, 'utf8');
      return newId;
    } catch (error) {
      this.#lc?.debug?.('Unable to get or set filesystem ID:', error);
      return 'unknown';
    }
  }

  #isInContainer(): boolean {
    try {
      if (process.env.ZERO_IN_CONTAINER) {
        return true;
      }

      if (existsSync('/.dockerenv')) {
        return true;
      }

      if (existsSync('/usr/local/bin/docker-entrypoint.sh')) {
        return true;
      }

      if (process.env.KUBERNETES_SERVICE_HOST) {
        return true;
      }

      if (
        process.env.DOCKER_CONTAINER_ID ||
        process.env.HOSTNAME?.match(/^[a-f0-9]{12}$/)
      ) {
        return true;
      }

      if (existsSync('/proc/1/cgroup')) {
        const cgroup = readFileSync('/proc/1/cgroup', 'utf8');
        if (
          cgroup.includes('docker') ||
          cgroup.includes('kubepods') ||
          cgroup.includes('containerd')
        ) {
          return true;
        }
      }

      return false;
    } catch (error) {
      this.#lc?.debug?.('Unable to detect container environment:', error);
      return false;
    }
  }
}

const manager = () => AnonymousTelemetryManager.getInstance();

export const startAnonymousTelemetry = (lc?: LogContext, config?: ZeroConfig) =>
  manager().start(lc, config);
export const recordMutation = (count = 1) => manager().recordMutation(count);
export const recordRowsSynced = (count: number) =>
  manager().recordRowsSynced(count);
export const recordConnectionSuccess = () =>
  manager().recordConnectionSuccess();
export const recordConnectionAttempted = () =>
  manager().recordConnectionAttempted();
export const shutdownAnonymousTelemetry = () => manager().shutdown();
