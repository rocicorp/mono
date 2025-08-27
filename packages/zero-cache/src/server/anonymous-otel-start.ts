import type {ObservableResult} from '@opentelemetry/api';
import {type Meter} from '@opentelemetry/api';
import {OTLPMetricExporter} from '@opentelemetry/exporter-metrics-otlp-http';
import {resourceFromAttributes} from '@opentelemetry/resources';
import {
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import type {LogContext} from '@rocicorp/logger';
import {execSync} from 'child_process';
import {randomUUID} from 'crypto';
import {existsSync, mkdirSync, readFileSync, writeFileSync} from 'fs';
import {homedir, platform} from 'os';
import {dirname, join} from 'path';
import {h64} from '../../../shared/src/hash.js';
import {
  getServerVersion,
  getZeroConfig,
  type ZeroConfig,
} from '../config/zero-config.js';

class AnonymousTelemetryManager {
  static #instance: AnonymousTelemetryManager;
  #starting = false;
  #stopped = false;
  #meter!: Meter;
  #meterProvider!: MeterProvider;
  #totalCrudMutations = 0;
  #totalCustomMutations = 0;
  #totalRowsSynced = 0;
  #totalConnectionsSuccess = 0;
  #totalConnectionsAttempted = 0;
  #activeClientGroupsGetter: (() => number) | undefined;
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
    this.#lc = lc;
    if (!config) {
      try {
        config = getZeroConfig();
      } catch (e) {
        this.#lc?.info?.('telemetry: disabled - unable to parse config', e);
        return;
      }
    }

    if (process.env.DO_NOT_TRACK) {
      this.#lc?.info?.(
        'telemetry: disabled - DO_NOT_TRACK environment variable is set',
      );
      return;
    }

    if (!config.enableTelemetry) {
      this.#lc?.info?.('telemetry: disabled - enableTelemetry is false');
      return;
    }

    if (this.#starting) {
      return;
    }

    this.#starting = true;
    this.#config = config;
    this.#viewSyncerCount = config.numSyncWorkers ?? 1;
    this.#cachedAttributes = undefined;

    this.#lc?.info?.(`telemetry: starting in 1 minute`);

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
      `telemetry: started (exports every ${60 * this.#viewSyncerCount} seconds for ${this.#viewSyncerCount} view-syncers)`,
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
    const crudMutationsCounter = this.#meter.createObservableCounter(
      'zero.crud_mutations_processed',
      {
        description: 'Total number of CRUD mutations processed',
      },
    );
    const customMutationsCounter = this.#meter.createObservableCounter(
      'zero.custom_mutations_processed',
      {
        description: 'Total number of custom mutations processed',
      },
    );
    const totalMutationsCounter = this.#meter.createObservableCounter(
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

    const activeClientGroupsGauge = this.#meter.createObservableGauge(
      'zero.gauge_active_client_groups',
      {
        description: 'Number of currently active client groups',
      },
    );

    // Callbacks
    const attrs = this.#getAttributes();
    uptimeGauge.addCallback((result: ObservableResult) => {
      const uptimeSeconds = Math.floor(process.uptime());
      result.observe(uptimeSeconds, attrs);
      this.#lc?.debug?.(`telemetry: uptime=${uptimeSeconds}s`);
    });
    uptimeCounter.addCallback((result: ObservableResult) => {
      const uptimeSeconds = Math.floor(process.uptime());
      result.observe(uptimeSeconds, attrs);
      this.#lc?.debug?.(`telemetry: uptime_counter=${uptimeSeconds}s`);
    });
    crudMutationsCounter.addCallback((result: ObservableResult) => {
      result.observe(this.#totalCrudMutations, attrs);
      this.#lc?.debug?.(
        `telemetry: crud_mutations=${this.#totalCrudMutations}`,
      );
    });
    customMutationsCounter.addCallback((result: ObservableResult) => {
      result.observe(this.#totalCustomMutations, attrs);
      this.#lc?.debug?.(
        `telemetry: custom_mutations=${this.#totalCustomMutations}`,
      );
    });
    totalMutationsCounter.addCallback((result: ObservableResult) => {
      const totalMutations =
        this.#totalCrudMutations + this.#totalCustomMutations;
      result.observe(totalMutations, attrs);
      this.#lc?.debug?.(`telemetry: total_mutations=${totalMutations}`);
    });
    rowsSyncedCounter.addCallback((result: ObservableResult) => {
      result.observe(this.#totalRowsSynced, attrs);
      this.#lc?.debug?.(`telemetry: rows_synced=${this.#totalRowsSynced}`);
    });
    connectionsSuccessCounter.addCallback((result: ObservableResult) => {
      result.observe(this.#totalConnectionsSuccess, attrs);
      this.#lc?.debug?.(
        `telemetry: connections_success=${this.#totalConnectionsSuccess}`,
      );
    });
    connectionsAttemptedCounter.addCallback((result: ObservableResult) => {
      result.observe(this.#totalConnectionsAttempted, attrs);
      this.#lc?.debug?.(
        `telemetry: connections_attempted=${this.#totalConnectionsAttempted}`,
      );
    });
    activeClientGroupsGauge.addCallback((result: ObservableResult) => {
      const activeClientGroups = this.#activeClientGroupsGetter?.() ?? 0;
      result.observe(activeClientGroups, attrs);
      this.#lc?.debug?.(
        `telemetry: gauge_active_client_groups=${activeClientGroups}`,
      );
    });
  }

  recordMutation(type: 'crud' | 'custom', count = 1) {
    if (type === 'crud') {
      this.#totalCrudMutations += count;
    } else {
      this.#totalCustomMutations += count;
    }
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

  setActiveClientGroupsGetter(getter: () => number) {
    this.#activeClientGroupsGetter = getter;
  }

  shutdown() {
    this.#stopped = true;
    if (this.#meterProvider) {
      this.#lc?.info?.('telemetry: shutting down');
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
        'zero.version': getServerVersion(this.#config),
        'zero.task.id': this.#config?.taskID || 'unknown',
        'zero.project.id': this.#getGitProjectId(),
        'zero.process.id': this.#processId,
        'zero.fs.id': this.#getOrSetFsID(),
      };
      this.#lc?.debug?.(
        `telemetry: cached attributes=${JSON.stringify(this.#cachedAttributes)}`,
      );
    }
    return this.#cachedAttributes;
  }

  #getPlatform(): string {
    if (process.env.FLY_APP_NAME || process.env.FLY_REGION) return 'fly.io';
    if (
      process.env.ECS_CONTAINER_METADATA_URI_V4 ||
      process.env.ECS_CONTAINER_METADATA_URI ||
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
    if (process.env.CONTAINER_APP_REVISION) return 'azure';
    if (process.env.FLIGHTCONTROL || process.env.FC_URL) return 'flightcontrol';
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
      this.#lc?.debug?.('telemetry: unable to get Git root commit:', error);
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

      mkdirSync(fsidDir, {recursive: true});

      // Always try atomic file creation first - this eliminates any race conditions
      const newId = randomUUID();
      try {
        writeFileSync(fsidPath, newId, {encoding: 'utf8', flag: 'wx'});
        return newId;
      } catch (writeError) {
        if ((writeError as NodeJS.ErrnoException).code === 'EEXIST') {
          const existingId = readFileSync(fsidPath, 'utf8').trim();
          return existingId;
        }
        throw writeError;
      }
    } catch (error) {
      this.#lc?.debug?.(
        'telemetry: unable to get or set filesystem ID:',
        error,
      );
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
      this.#lc?.debug?.(
        'telemetry: unable to detect container environment:',
        error,
      );
      return false;
    }
  }
}

const manager = () => AnonymousTelemetryManager.getInstance();

export const startAnonymousTelemetry = (lc?: LogContext, config?: ZeroConfig) =>
  manager().start(lc, config);
export const recordMutation = (type: 'crud' | 'custom', count = 1) =>
  manager().recordMutation(type, count);
export const recordRowsSynced = (count: number) =>
  manager().recordRowsSynced(count);
export const recordConnectionSuccess = () =>
  manager().recordConnectionSuccess();
export const recordConnectionAttempted = () =>
  manager().recordConnectionAttempted();
export const setActiveClientGroupsGetter = (getter: () => number) =>
  manager().setActiveClientGroupsGetter(getter);
export const shutdownAnonymousTelemetry = () => manager().shutdown();
