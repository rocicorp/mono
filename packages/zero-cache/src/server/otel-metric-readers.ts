import {Metadata} from '@grpc/grpc-js';
import {diag} from '@opentelemetry/api';
import {OTLPMetricExporter as OTLPGrpcMetricExporter} from '@opentelemetry/exporter-metrics-otlp-grpc';
import {
  AggregationTemporalityPreference,
  OTLPMetricExporter as OTLPJsonMetricExporter,
} from '@opentelemetry/exporter-metrics-otlp-http';
import {OTLPMetricExporter as OTLPProtoMetricExporter} from '@opentelemetry/exporter-metrics-otlp-proto';
import {CompressionAlgorithm} from '@opentelemetry/otlp-exporter-base';
import {
  PeriodicExportingMetricReader,
  type PushMetricExporter,
} from '@opentelemetry/sdk-metrics';

const SECONDARY_ENV = {
  endpoint: 'SECONDARY_OTEL_EXPORTER_OTLP_METRICS_ENDPOINT',
  protocol: 'SECONDARY_OTEL_EXPORTER_OTLP_METRICS_PROTOCOL',
  headers: 'SECONDARY_OTEL_EXPORTER_OTLP_METRICS_HEADERS',
  compression: 'SECONDARY_OTEL_EXPORTER_OTLP_METRICS_COMPRESSION',
  temporality: 'SECONDARY_OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE',
  interval: 'SECONDARY_OTEL_METRIC_EXPORT_INTERVAL',
  timeout: 'SECONDARY_OTEL_METRIC_EXPORT_TIMEOUT',
} as const;

const UNSUPPORTED_SECONDARY_ENV = [
  'SECONDARY_OTEL_EXPORTER_OTLP_ENDPOINT',
  'SECONDARY_OTEL_EXPORTER_OTLP_PROTOCOL',
  'SECONDARY_OTEL_EXPORTER_OTLP_HEADERS',
] as const;

const DEFAULT_EXPORT_INTERVAL = 60_000;
const DEFAULT_EXPORT_TIMEOUT = 30_000;
const MIN_EXPORT_INTERVAL = 15_000;
const MAX_ENDPOINT_LENGTH = 2_048;
const MAX_HEADERS_LENGTH = 8_192;
const MAX_HEADER_COUNT = 64;
const MAX_HEADER_NAME_LENGTH = 256;
const MAX_HEADER_VALUE_LENGTH = 4_096;
const HEADER_NAME_PATTERN = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;
const GRPC_HEADER_NAME_PATTERN = /^[0-9a-z_.-]+$/;
const GRPC_HEADER_VALUE_PATTERN = /^[\x20-\x7e]*$/;
const OTLP_ENV_PREFIX = 'OTEL_EXPORTER_OTLP_';

type OTLPProtocol = 'grpc' | 'http/json' | 'http/protobuf';
type TemporalityPreference = 'cumulative' | 'delta' | 'lowmemory';

type SecondaryMetricReaderConfig = Readonly<{
  endpoint: string;
  protocol: OTLPProtocol;
  headers: Readonly<Record<string, string>>;
  compression: CompressionAlgorithm | undefined;
  temporality: TemporalityPreference | undefined;
  interval: number;
  timeout: number;
}>;

export function createMetricReadersFromEnv():
  | [PeriodicExportingMetricReader, PeriodicExportingMetricReader]
  | undefined {
  const secondary = parseSecondaryMetricReaderConfig(process.env);
  if (!secondary) {
    return undefined;
  }

  validatePrimaryMetricExporterConfig();
  return [
    createPrimaryMetricReader(),
    new PeriodicExportingMetricReader({
      exporter: withoutPrimaryExporterEnv(() =>
        createSecondaryMetricExporter(secondary),
      ),
      exportIntervalMillis: secondary.interval,
      exportTimeoutMillis: secondary.timeout,
    }),
  ];
}

export function parseSecondaryMetricReaderConfig(
  env: NodeJS.ProcessEnv,
): SecondaryMetricReaderConfig | undefined {
  const unsupported = UNSUPPORTED_SECONDARY_ENV.filter(
    name => env[name] !== undefined,
  );
  if (unsupported.length > 0) {
    throw new Error(
      `Unsupported secondary OTLP variable: ${unsupported.join(', ')}`,
    );
  }

  const rawEndpoint = env[SECONDARY_ENV.endpoint]?.trim();
  if (!rawEndpoint) {
    return undefined;
  }

  const interval = parsePositiveInteger(
    env,
    SECONDARY_ENV.interval,
    DEFAULT_EXPORT_INTERVAL,
  );
  if (interval < MIN_EXPORT_INTERVAL) {
    throw new Error(
      `${SECONDARY_ENV.interval} must be at least ${MIN_EXPORT_INTERVAL}`,
    );
  }

  const protocol = parseEnum(
    env,
    SECONDARY_ENV.protocol,
    ['grpc', 'http/protobuf', 'http/json'] as const,
    'http/protobuf',
  );
  return Object.freeze({
    endpoint: parseEndpoint(rawEndpoint),
    protocol,
    headers: parseHeaders(env[SECONDARY_ENV.headers], protocol),
    compression: parseOptionalEnum(env, SECONDARY_ENV.compression, [
      CompressionAlgorithm.NONE,
      CompressionAlgorithm.GZIP,
    ] as const),
    temporality: parseOptionalEnum(env, SECONDARY_ENV.temporality, [
      'cumulative',
      'delta',
      'lowmemory',
    ] as const),
    interval,
    timeout: Math.min(
      parsePositiveInteger(env, SECONDARY_ENV.timeout, DEFAULT_EXPORT_TIMEOUT),
      interval,
    ),
  });
}

function validatePrimaryMetricExporterConfig(): void {
  if (
    !process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT?.trim() &&
    !process.env.OTEL_EXPORTER_OTLP_ENDPOINT?.trim()
  ) {
    throw new Error(
      `${SECONDARY_ENV.endpoint} requires a primary OTEL_EXPORTER_OTLP_METRICS_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT`,
    );
  }

  const exporters = new Set(
    (process.env.OTEL_METRICS_EXPORTER ?? '')
      .split(',')
      .map(value => value.trim())
      .filter(Boolean),
  );
  if (exporters.size > 0 && (exporters.size !== 1 || !exporters.has('otlp'))) {
    throw new Error(
      `${SECONDARY_ENV.endpoint} requires OTEL_METRICS_EXPORTER=otlp`,
    );
  }
}

function createPrimaryMetricReader(): PeriodicExportingMetricReader {
  const interval = positiveNumberFromOtelEnv(
    'OTEL_METRIC_EXPORT_INTERVAL',
    DEFAULT_EXPORT_INTERVAL,
  );
  return new PeriodicExportingMetricReader({
    exporter: createPrimaryMetricExporter(),
    exportIntervalMillis: interval,
    exportTimeoutMillis: Math.min(
      positiveNumberFromOtelEnv(
        'OTEL_METRIC_EXPORT_TIMEOUT',
        DEFAULT_EXPORT_TIMEOUT,
      ),
      interval,
    ),
  });
}

function createPrimaryMetricExporter(): PushMetricExporter {
  const protocol =
    process.env.OTEL_EXPORTER_OTLP_METRICS_PROTOCOL?.trim() ||
    process.env.OTEL_EXPORTER_OTLP_PROTOCOL?.trim() ||
    'http/protobuf';
  switch (protocol) {
    case 'grpc':
      return new OTLPGrpcMetricExporter();
    case 'http/json':
      return new OTLPJsonMetricExporter();
    case 'http/protobuf':
      return new OTLPProtoMetricExporter();
    default:
      diag.warn(
        `Unsupported OTLP metrics protocol: "${protocol}". Using http/protobuf.`,
      );
      return new OTLPProtoMetricExporter();
  }
}

function createSecondaryMetricExporter(
  config: SecondaryMetricReaderConfig,
): PushMetricExporter {
  const options = {
    url: config.endpoint,
    ...(config.compression === undefined
      ? {}
      : {compression: config.compression}),
    ...(config.temporality === undefined
      ? {}
      : {
          temporalityPreference: aggregationTemporalityPreference(
            config.temporality,
          ),
        }),
  };

  switch (config.protocol) {
    case 'grpc': {
      const metadata = new Metadata();
      for (const [name, value] of Object.entries(config.headers)) {
        metadata.set(name, value);
      }
      return new OTLPGrpcMetricExporter({...options, metadata});
    }
    case 'http/json':
      return new OTLPJsonMetricExporter({...options, headers: config.headers});
    case 'http/protobuf':
      return new OTLPProtoMetricExporter({
        ...options,
        headers: config.headers,
      });
  }
}

// Exporter constructors merge OTEL_EXPORTER_OTLP_* into explicit options.
function withoutPrimaryExporterEnv<T>(create: () => T): T {
  const saved = new Map<string, string>();
  for (const name of Object.keys(process.env)) {
    if (name.startsWith(OTLP_ENV_PREFIX)) {
      const value = process.env[name];
      if (value !== undefined) {
        saved.set(name, value);
      }
      delete process.env[name];
    }
  }
  try {
    return create();
  } finally {
    for (const name of Object.keys(process.env)) {
      if (name.startsWith(OTLP_ENV_PREFIX)) {
        delete process.env[name];
      }
    }
    for (const [name, value] of saved) {
      process.env[name] = value;
    }
  }
}

function parseEndpoint(raw: string): string {
  if (raw.length > MAX_ENDPOINT_LENGTH) {
    throw new Error(
      `${SECONDARY_ENV.endpoint} exceeds ${MAX_ENDPOINT_LENGTH} characters`,
    );
  }
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    throw new Error(
      `${SECONDARY_ENV.endpoint} must be an absolute HTTP or HTTPS URL`,
    );
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error(
      `${SECONDARY_ENV.endpoint} must be an absolute HTTP or HTTPS URL`,
    );
  }
  if (parsed.username || parsed.password) {
    throw new Error(`${SECONDARY_ENV.endpoint} must not contain credentials`);
  }
  return parsed.toString();
}

function parseHeaders(
  raw: string | undefined,
  protocol: OTLPProtocol,
): Readonly<Record<string, string>> {
  if (!raw) {
    return Object.freeze({});
  }
  if (raw.length > MAX_HEADERS_LENGTH) {
    throw new Error(
      `${SECONDARY_ENV.headers} exceeds ${MAX_HEADERS_LENGTH} characters`,
    );
  }

  const rawEntries = raw.split(',');
  if (rawEntries.length > MAX_HEADER_COUNT) {
    throw new Error(
      `${SECONDARY_ENV.headers} exceeds ${MAX_HEADER_COUNT} headers`,
    );
  }

  const entries = new Map<string, string>();
  for (const rawEntry of rawEntries) {
    const separator = rawEntry.indexOf('=');
    if (separator < 1) {
      throw new Error(`${SECONDARY_ENV.headers} contains an invalid header`);
    }

    let name: string;
    let value: string;
    try {
      name = decodeURIComponent(rawEntry.slice(0, separator).trim());
      value = decodeURIComponent(rawEntry.slice(separator + 1).trim());
    } catch {
      throw new Error(`${SECONDARY_ENV.headers} contains an invalid header`);
    }

    const normalizedName = name.toLowerCase();
    if (
      name.length > MAX_HEADER_NAME_LENGTH ||
      !HEADER_NAME_PATTERN.test(name) ||
      (protocol === 'grpc' &&
        (!GRPC_HEADER_NAME_PATTERN.test(normalizedName) ||
          normalizedName.endsWith('-bin'))) ||
      entries.has(normalizedName)
    ) {
      throw new Error(
        `${SECONDARY_ENV.headers} contains an invalid header name`,
      );
    }
    if (
      value.length === 0 ||
      value.length > MAX_HEADER_VALUE_LENGTH ||
      value.includes('\r') ||
      value.includes('\n') ||
      (protocol === 'grpc' && !GRPC_HEADER_VALUE_PATTERN.test(value))
    ) {
      throw new Error(
        `${SECONDARY_ENV.headers} contains an invalid header value`,
      );
    }
    entries.set(normalizedName, value);
  }
  return Object.freeze(Object.fromEntries(entries));
}

function parsePositiveInteger(
  env: NodeJS.ProcessEnv,
  name: string,
  fallback: number,
): number {
  const raw = env[name];
  if (raw === undefined || raw.trim() === '') {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

function positiveNumberFromOtelEnv(name: string, fallback: number): number {
  const value = Number(process.env[name]);
  return value > 0 ? value : fallback;
}

function parseOptionalEnum<const Values extends readonly string[]>(
  env: NodeJS.ProcessEnv,
  name: string,
  values: Values,
): Values[number] | undefined {
  const value = env[name]?.trim();
  if (!value) {
    return undefined;
  }
  if (!(values as readonly string[]).includes(value)) {
    throw new Error(`${name} must be one of ${values.join(', ')}`);
  }
  return value as Values[number];
}

function parseEnum<const Values extends readonly string[]>(
  env: NodeJS.ProcessEnv,
  name: string,
  values: Values,
  fallback: Values[number],
): Values[number] {
  return parseOptionalEnum(env, name, values) ?? fallback;
}

function aggregationTemporalityPreference(
  value: TemporalityPreference,
): AggregationTemporalityPreference {
  switch (value) {
    case 'cumulative':
      return AggregationTemporalityPreference.CUMULATIVE;
    case 'delta':
      return AggregationTemporalityPreference.DELTA;
    case 'lowmemory':
      return AggregationTemporalityPreference.LOWMEMORY;
  }
}
