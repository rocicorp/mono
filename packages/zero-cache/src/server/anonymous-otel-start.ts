import {type Meter} from '@opentelemetry/api';
import {OTLPMetricExporter} from '@opentelemetry/exporter-metrics-otlp-http';
import {PeriodicExportingMetricReader} from '@opentelemetry/sdk-metrics';
import {MeterProvider} from '@opentelemetry/sdk-metrics';
import {resourceFromAttributes} from '@opentelemetry/resources';
import type {ObservableResult} from '@opentelemetry/api';
import {platform} from 'os';
import {h64} from '../../../shared/src/hash.js';
import type {LogContext} from '@rocicorp/logger';
import packageJson from '../../package.json' with {type: 'json'};
import {getZeroConfig, type ZeroConfig} from '../config/zero-config.js';

const ROCICORP_TELEMETRY_TOKEN =
  process.env.ROCICORP_TELEMETRY_TOKEN || 'anonymous-token';

class AnonymousTelemetryManager {
  static #instance: AnonymousTelemetryManager;
  #started = false;
  #meter!: Meter;
  #meterProvider!: MeterProvider;
  #startTime = Date.now();
  #lastMinuteMutations = 0;
  #lastMinuteRowsSynced = 0;
  #connectedClientGroups = new Set<string>();
  #activeQueries = new Map<string, Set<string>>();
  #cvrSize = 0;
  #lc: LogContext | undefined;
  #config: ZeroConfig | undefined;

  private constructor() {}

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
        // Gracefully handle cases where config cannot be parsed (e.g., in test environments)
        this.#lc?.debug?.('Anonymous telemetry disabled: unable to parse config', e);
        return;
      }
    }
    
    if (this.#started || !config.enableUsageAnalytics) {
      return;
    }
    this.#lc = lc;
    this.#config = config;

    const resource = resourceFromAttributes(this.#getAttributes());
    const metricReader = new PeriodicExportingMetricReader({
      exportIntervalMillis: 60000,
      exporter: new OTLPMetricExporter({
        url: 'https://otlp-gateway-prod-us-east-2.grafana.net/otlp/v1/metrics',
        headers: {authorization: `Bearer ${ROCICORP_TELEMETRY_TOKEN}`},
      }),
    });

    this.#meterProvider = new MeterProvider({
      resource,
      readers: [metricReader],
    });
    this.#meter = this.#meterProvider.getMeter('zero-anonymous-telemetry');

    this.#setupMetrics();
    this.#lc?.info?.('Anonymous telemetry started');
    this.#started = true;
  }

  #setupMetrics() {
    // Observable gauges
    const uptimeGauge = this.#meter.createObservableGauge('zero.uptime', {
      description: 'System uptime in seconds',
      unit: 'seconds',
    });
    const clientGroupsGauge = this.#meter.createObservableGauge(
      'zero.client_groups',
      {
        description: 'Number of connected client groups',
      },
    );
    const activeQueriesGauge = this.#meter.createObservableGauge(
      'zero.active_queries',
      {
        description: 'Total number of active queries across all client groups',
      },
    );
    const activeQueriesPerClientGroupGauge = this.#meter.createObservableGauge(
      'zero.active_queries_per_client_group',
      {description: 'Number of active queries per client group'},
    );
    const cvrSizeGauge = this.#meter.createObservableGauge('zero.cvr_size', {
      description: 'Current CVR size in bytes',
      unit: 'bytes',
    });

    // Observable counters
    const mutationsCounter = this.#meter.createObservableCounter(
      'zero.mutations_processed',
      {
        description: 'Number of mutations processed in the last minute',
      },
    );
    const rowsSyncedCounter = this.#meter.createObservableCounter(
      'zero.rows_synced',
      {
        description: 'Number of rows synced in the last minute',
      },
    );

    // Callbacks
    const attrs = this.#getAttributes();
    uptimeGauge.addCallback((result: ObservableResult) => {
      result.observe(Math.floor((Date.now() - this.#startTime) / 1000), attrs);
    });
    clientGroupsGauge.addCallback((result: ObservableResult) => {
      result.observe(this.#connectedClientGroups.size, attrs);
    });
    activeQueriesGauge.addCallback((result: ObservableResult) => {
      result.observe(this.#getTotalActiveQueries(), attrs);
    });
    activeQueriesPerClientGroupGauge.addCallback((result: ObservableResult) => {
      for (const [clientGroupID, queries] of this.#activeQueries) {
        result.observe(queries.size, {
          ...attrs,
          'zero.client_group.id': clientGroupID,
        });
      }
    });
    cvrSizeGauge.addCallback((result: ObservableResult) => {
      result.observe(this.#cvrSize, attrs);
    });
    mutationsCounter.addCallback((result: ObservableResult) => {
      result.observe(this.#lastMinuteMutations, attrs);
      this.#lastMinuteMutations = 0;
    });
    rowsSyncedCounter.addCallback((result: ObservableResult) => {
      result.observe(this.#lastMinuteRowsSynced, attrs);
      this.#lastMinuteRowsSynced = 0;
    });
  }

  recordMutation() {
    this.#lastMinuteMutations++;
  }

  recordRowsSynced(count: number) {
    this.#lastMinuteRowsSynced += count;
  }

  addActiveQuery(clientGroupID: string, queryID: string) {
    if (!this.#activeQueries.has(clientGroupID)) {
      this.#activeQueries.set(clientGroupID, new Set());
    }
    this.#activeQueries.get(clientGroupID)!.add(queryID);
  }

  removeActiveQuery(clientGroupID: string, queryID: string) {
    const queries = this.#activeQueries.get(clientGroupID);
    if (queries) {
      queries.delete(queryID);
      if (queries.size === 0) {
        this.#activeQueries.delete(clientGroupID);
      }
    }
  }

  updateCvrSize(sizeBytes: number) {
    this.#cvrSize = sizeBytes;
  }

  addClientGroup(clientGroupID: string) {
    this.#connectedClientGroups.add(clientGroupID);
  }

  removeClientGroup(clientGroupID: string) {
    this.#connectedClientGroups.delete(clientGroupID);
    this.#activeQueries.delete(clientGroupID);
  }

  shutdown() {
    if (this.#meterProvider) {
      this.#lc?.info?.('Shutting down anonymous telemetry');
      void this.#meterProvider.shutdown();
    }
  }

  #getAttributes() {
    return {
      'zero.app.id': h64(this.#config?.upstream.db || 'unknown').toString(),
      'zero.machine.os': platform(),
      'zero.telemetry.type': 'anonymous',
      'zero.infra.platform': this.#getPlatform(),
      'zero.version': this.#config?.serverVersion ?? packageJson.version,
    };
  }

  #getPlatform(): string {
    if (process.env.FLY_APP_NAME || process.env.FLY_REGION) return 'fly.io';
    if (
      process.env.AWS_LAMBDA_FUNCTION_NAME ||
      process.env.AWS_REGION ||
      process.env.AWS_EXECUTION_ENV
    )
      return 'aws';
    if (process.env.RAILWAY_ENV || process.env.RAILWAY_STATIC_URL)
      return 'railway';
    if (process.env.RENDER || process.env.RENDER_SERVICE_ID) return 'render';
    return 'local';
  }

  #getTotalActiveQueries(): number {
    let total = 0;
    for (const queries of this.#activeQueries.values()) {
      total += queries.size;
    }
    return total;
  }
}

const manager = () => AnonymousTelemetryManager.getInstance();

export const startAnonymousTelemetry = (lc?: LogContext, config?: ZeroConfig) => manager().start(lc, config);
export const recordMutation = () => manager().recordMutation();
export const recordRowsSynced = (count: number) =>
  manager().recordRowsSynced(count);
export const addActiveQuery = (clientGroupID: string, queryID: string) =>
  manager().addActiveQuery(clientGroupID, queryID);
export const removeActiveQuery = (clientGroupID: string, queryID: string) =>
  manager().removeActiveQuery(clientGroupID, queryID);
export const updateCvrSize = (sizeBytes: number) =>
  manager().updateCvrSize(sizeBytes);
export const addClientGroup = (clientGroupID: string) =>
  manager().addClientGroup(clientGroupID);
export const removeClientGroup = (clientGroupID: string) =>
  manager().removeClientGroup(clientGroupID);
export const shutdownAnonymousTelemetry = () => manager().shutdown();
