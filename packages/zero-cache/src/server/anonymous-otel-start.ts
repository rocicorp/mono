import {type Meter} from '@opentelemetry/api';
import {OTLPMetricExporter} from '@opentelemetry/exporter-metrics-otlp-http';
import {PeriodicExportingMetricReader} from '@opentelemetry/sdk-metrics';
import {MeterProvider} from '@opentelemetry/sdk-metrics';
import {resourceFromAttributes} from '@opentelemetry/resources';
import type {ObservableGauge, ObservableCounter, ObservableResult} from '@opentelemetry/api';
import {platform} from 'os';
import { hash } from 'crypto';

const ROCICORP_TELEMETRY_TOKEN = process.env.ROCICORP_TELEMETRY_TOKEN || 'anonymous-token';

// Simple telemetry config function
function getTelemetryConfig(env: NodeJS.ProcessEnv) {
  return {
    optOut: env.ZERO_TELEMETRY_OPT_OUT === 'true' || env.ZERO_TELEMETRY_OPT_OUT === '1'
  };
}

class AnonymousTelemetryManager {
  static #instance: AnonymousTelemetryManager;
  #started = false;
  #meter!: Meter;
  #meterProvider!: MeterProvider;
  #startTime: number;
  #lastMinuteMutations: number = 0;
  #lastMinuteRowsSynced: number = 0;
  #connectedClientGroups: Set<string> = new Set();
  #uptimeGauge!: ObservableGauge;
  #clientGroupsGauge!: ObservableGauge;
  #mutationsCounter!: ObservableCounter;
  #rowsSyncedCounter!: ObservableCounter;

  private constructor() {
    this.#startTime = Date.now();
  }

  static getInstance(): AnonymousTelemetryManager {
    if (!AnonymousTelemetryManager.#instance) {
      AnonymousTelemetryManager.#instance = new AnonymousTelemetryManager();
    }
    return AnonymousTelemetryManager.#instance;
  }

  startAnonymousTelemetry() {
    if (this.#started) {
      return;
    }
    this.#started = true;

    const telemetryConfig = getTelemetryConfig(process.env);
    if (telemetryConfig.optOut) {
      return;
    }

    // Create a separate MeterProvider for anonymous telemetry
    // This won't interfere with the main SDK
    const resource = resourceFromAttributes({
      //hash of upstream db uri
      'zero.project.id': hash(process.env.ZERO_UPSTREAM_DB as string || 'unknown', 'sha256').toString(),
      'zero.machine.id': process.env.ZERO_MACHINE_ID || 'unknown',
      'zero.machine.os': platform(),
      'zero.telemetry.type': 'anonymous'
    });

    const metricReader = new PeriodicExportingMetricReader({
      exportIntervalMillis: 60000,
      exporter: new OTLPMetricExporter({
        url: 'https://otlp-gateway-prod-us-east-2.grafana.net/otlp/v1/metrics',
        headers: {
          authorization: 'Bearer ' + ROCICORP_TELEMETRY_TOKEN
        }
      })
    });

    this.#meterProvider = new MeterProvider({
      resource,
      readers: [metricReader]
    });

    this.#meter = this.#meterProvider.getMeter('zero-anonymous-telemetry');

    this.#uptimeGauge = this.#meter.createObservableGauge('zero.uptime', {
      description: 'System uptime in seconds',
      unit: 'seconds'
    });

    this.#clientGroupsGauge = this.#meter.createObservableGauge('zero.client_groups', {
      description: 'Number of connected client groups'
    });

    this.#mutationsCounter = this.#meter.createObservableCounter('zero.mutations_processed', {
      description: 'Number of mutations processed in the last minute'
    });

    this.#rowsSyncedCounter = this.#meter.createObservableCounter('zero.rows_synced', {
      description: 'Number of rows synced in the last minute'
    });

    // Set up observable callbacks that will be called every 60 seconds
    this.#uptimeGauge.addCallback((result: ObservableResult) => {
      const uptimeSeconds = Math.floor((Date.now() - this.#startTime) / 1000);
      result.observe(uptimeSeconds, this.#getAttributes());
    });

    this.#clientGroupsGauge.addCallback((result: ObservableResult) => {
      result.observe(this.#getConnectedClientGroups(), this.#getAttributes());
    });

    this.#mutationsCounter.addCallback((result: ObservableResult) => {
      result.observe(this.#lastMinuteMutations, this.#getAttributes());
      // Reset counter after observation
      this.#lastMinuteMutations = 0;
    });

    this.#rowsSyncedCounter.addCallback((result: ObservableResult) => {
      result.observe(this.#lastMinuteRowsSynced, this.#getAttributes());
      // Reset counter after observation
      this.#lastMinuteRowsSynced = 0;
    });
  }

  // Methods to record events as they happen
  recordMutation() {
    if (this.#started) {
      this.#lastMinuteMutations++;
    }
  }

  recordRowsSynced(count: number) {
    if (this.#started) {
      this.#lastMinuteRowsSynced += count;
    }
  }

  // Client group tracking methods
  addClientGroup(clientGroupID: string) {
    if (this.#started) {
      this.#connectedClientGroups.add(clientGroupID);
    }
  }

  removeClientGroup(clientGroupID: string) {
    if (this.#started) {
      this.#connectedClientGroups.delete(clientGroupID);
    }
  }

  shutdown() {
    if (this.#meterProvider) {
      void this.#meterProvider.shutdown();
    }
  }

  #getAttributes() {
    return {
      'zero.machine.id': process.env.ZERO_MACHINE_ID || 'unknown',
      'zero.machine.os': platform()
    };
  }


  #getConnectedClientGroups(): number {
    return this.#connectedClientGroups.size;
  }
}

export const startAnonymousTelemetry = () => {
  AnonymousTelemetryManager.getInstance().startAnonymousTelemetry();
};

export const recordMutation = () => {
  AnonymousTelemetryManager.getInstance().recordMutation();
};

export const recordRowsSynced = (count: number) => {
  AnonymousTelemetryManager.getInstance().recordRowsSynced(count);
};

export const addClientGroup = (clientGroupID: string) => {
  AnonymousTelemetryManager.getInstance().addClientGroup(clientGroupID);
};

export const removeClientGroup = (clientGroupID: string) => {
  AnonymousTelemetryManager.getInstance().removeClientGroup(clientGroupID);
};

export const shutdownAnonymousTelemetry = () => {
  AnonymousTelemetryManager.getInstance().shutdown();
};