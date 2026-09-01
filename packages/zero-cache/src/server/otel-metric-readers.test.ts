import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';

const constructors = vi.hoisted(() => ({
  grpc: vi.fn(),
  json: vi.fn(),
  proto: vi.fn(),
  reader: vi.fn(),
}));

vi.mock('@opentelemetry/exporter-metrics-otlp-grpc', () => ({
  OTLPMetricExporter: constructors.grpc,
}));

vi.mock('@opentelemetry/exporter-metrics-otlp-http', () => ({
  AggregationTemporalityPreference: {
    DELTA: 0,
    CUMULATIVE: 1,
    LOWMEMORY: 2,
  },
  OTLPMetricExporter: constructors.json,
}));

vi.mock('@opentelemetry/exporter-metrics-otlp-proto', () => ({
  OTLPMetricExporter: constructors.proto,
}));

vi.mock('@opentelemetry/otlp-exporter-base', () => ({
  CompressionAlgorithm: {NONE: 'none', GZIP: 'gzip'},
}));

vi.mock('@opentelemetry/sdk-metrics', () => ({
  PeriodicExportingMetricReader: constructors.reader,
}));

import {
  createMetricReadersFromEnv,
  parseSecondaryMetricReaderConfig,
} from './otel-metric-readers.ts';

const originalEnv = {...process.env};
const secondaryEndpointEnv = 'SECONDARY_OTEL_EXPORTER_OTLP_METRICS_ENDPOINT';

beforeEach(() => {
  constructors.grpc.mockReset().mockImplementation(function (options) {
    return {kind: 'grpc', options};
  });
  constructors.json.mockReset().mockImplementation(function (options) {
    return {kind: 'json', options};
  });
  constructors.proto.mockReset().mockImplementation(function (options) {
    return {kind: 'proto', options};
  });
  constructors.reader.mockReset().mockImplementation(function (options) {
    return {options};
  });
  process.env = {...originalEnv};
  for (const name of Object.keys(process.env)) {
    if (name.startsWith('OTEL_') || name.startsWith('SECONDARY_OTEL_')) {
      delete process.env[name];
    }
  }
});

afterEach(() => {
  process.env = {...originalEnv};
});

describe('secondary OTEL metric reader configuration', () => {
  test('is disabled without a secondary endpoint', () => {
    expect(parseSecondaryMetricReaderConfig(process.env)).toBeUndefined();
    expect(createMetricReadersFromEnv()).toBeUndefined();
    expect(constructors.reader).not.toHaveBeenCalled();
  });

  test('parses defaults and normalizes the endpoint', () => {
    process.env[secondaryEndpointEnv] = ' https://customer.example/v1/metrics ';

    expect(parseSecondaryMetricReaderConfig(process.env)).toEqual({
      endpoint: 'https://customer.example/v1/metrics',
      protocol: 'http/protobuf',
      headers: {},
      compression: undefined,
      temporality: undefined,
      interval: 60_000,
      timeout: 30_000,
    });
  });

  test('parses the supported configuration and clamps timeout to interval', () => {
    process.env[secondaryEndpointEnv] = 'https://customer.example/v1/metrics';
    process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_PROTOCOL = 'grpc';
    process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_HEADERS =
      'Authorization=Bearer%20secret,X-Tenant=tenant%2C1';
    process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_COMPRESSION = 'gzip';
    process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE =
      'delta';
    process.env.SECONDARY_OTEL_METRIC_EXPORT_INTERVAL = '15000';
    process.env.SECONDARY_OTEL_METRIC_EXPORT_TIMEOUT = '20000';

    expect(parseSecondaryMetricReaderConfig(process.env)).toEqual({
      endpoint: 'https://customer.example/v1/metrics',
      protocol: 'grpc',
      headers: {
        'authorization': 'Bearer secret',
        'x-tenant': 'tenant,1',
      },
      compression: 'gzip',
      temporality: 'delta',
      interval: 15_000,
      timeout: 15_000,
    });
  });

  test.each([
    ['SECONDARY_OTEL_EXPORTER_OTLP_ENDPOINT', 'https://generic.example'],
    ['SECONDARY_OTEL_EXPORTER_OTLP_PROTOCOL', 'grpc'],
    ['SECONDARY_OTEL_EXPORTER_OTLP_HEADERS', 'authorization=secret'],
  ])('rejects the unsupported generic alias %s', (name, value) => {
    process.env[name] = value;
    expect(() => parseSecondaryMetricReaderConfig(process.env)).toThrow(
      'Unsupported secondary OTLP variable',
    );
  });

  test.each([
    ['not a URL', 'absolute HTTP or HTTPS URL'],
    ['ftp://customer.example/metrics', 'absolute HTTP or HTTPS URL'],
    [
      'https://user:password@customer.example/metrics',
      'must not contain credentials',
    ],
  ])('rejects invalid endpoint %s without echoing it', (endpoint, message) => {
    process.env[secondaryEndpointEnv] = endpoint;
    expect(() => parseSecondaryMetricReaderConfig(process.env)).toThrow(
      message,
    );
    try {
      parseSecondaryMetricReaderConfig(process.env);
    } catch (error) {
      expect(String(error)).not.toContain(endpoint);
    }
  });

  test.each([
    ['SECONDARY_OTEL_EXPORTER_OTLP_METRICS_PROTOCOL', 'json'],
    ['SECONDARY_OTEL_EXPORTER_OTLP_METRICS_COMPRESSION', 'br'],
    [
      'SECONDARY_OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE',
      'unspecified',
    ],
    ['SECONDARY_OTEL_METRIC_EXPORT_INTERVAL', '14999'],
    ['SECONDARY_OTEL_METRIC_EXPORT_INTERVAL', 'infinity'],
    ['SECONDARY_OTEL_METRIC_EXPORT_TIMEOUT', '0'],
  ])('rejects invalid %s', (name, value) => {
    process.env[secondaryEndpointEnv] = 'https://customer.example/v1/metrics';
    process.env[name] = value;
    expect(() => parseSecondaryMetricReaderConfig(process.env)).toThrow(name);
  });

  test.each([
    'missing-equals',
    '=missing-name',
    'bad%20name=value',
    'name=',
    'name=bad%0Avalue',
    'name=value,NAME=duplicate',
    'name=%E0%A4%A',
  ])('rejects malformed headers without echoing their values: %s', headers => {
    process.env[secondaryEndpointEnv] = 'https://customer.example/v1/metrics';
    process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_HEADERS = headers;
    expect(() => parseSecondaryMetricReaderConfig(process.env)).toThrow(
      'SECONDARY_OTEL_EXPORTER_OTLP_METRICS_HEADERS',
    );
    try {
      parseSecondaryMetricReaderConfig(process.env);
    } catch (error) {
      expect(String(error)).not.toContain(headers);
    }
  });

  test('accepts header names that overlap object prototype properties', () => {
    process.env[secondaryEndpointEnv] = 'https://customer.example/v1/metrics';
    process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_HEADERS =
      '__proto__=prototype,toString=value';

    const parsed = parseSecondaryMetricReaderConfig(process.env);

    expect(parsed?.headers).toEqual({
      ['__proto__']: 'prototype',
      tostring: 'value',
    });
  });

  test.each([
    ['x&header=value', 'name'],
    ['token-bin=value', 'name'],
    ['name=t%C3%A9st', 'value'],
  ])('rejects a gRPC-incompatible header %s', (headers, invalidPart) => {
    process.env[secondaryEndpointEnv] = 'https://customer.example/v1/metrics';
    process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_PROTOCOL = 'grpc';
    process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_HEADERS = headers;

    expect(() => parseSecondaryMetricReaderConfig(process.env)).toThrow(
      `contains an invalid header ${invalidPart}`,
    );
  });
});

describe('primary and secondary metric reader construction', () => {
  test('creates exactly two readers and keeps the primary first', () => {
    process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT =
      'http://primary.example/v1/metrics';
    process.env.OTEL_EXPORTER_OTLP_METRICS_PROTOCOL = 'http/protobuf';
    process.env[secondaryEndpointEnv] = 'https://secondary.example/v1/metrics';

    const readers = createMetricReadersFromEnv();

    expect(readers).toHaveLength(2);
    expect(constructors.proto).toHaveBeenNthCalledWith(1);
    expect(constructors.proto).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({url: 'https://secondary.example/v1/metrics'}),
    );
    expect(constructors.reader).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        exporter: expect.objectContaining({kind: 'proto'}),
      }),
    );
    expect(constructors.reader).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        exporter: expect.objectContaining({kind: 'proto'}),
      }),
    );
  });

  test('uses signal-specific primary protocol precedence', () => {
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT = 'http://primary.example:4317';
    process.env.OTEL_EXPORTER_OTLP_PROTOCOL = 'http/json';
    process.env.OTEL_EXPORTER_OTLP_METRICS_PROTOCOL = 'grpc';
    process.env[secondaryEndpointEnv] = 'https://secondary.example/v1/metrics';
    process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_PROTOCOL = 'http/json';

    createMetricReadersFromEnv();

    expect(constructors.grpc).toHaveBeenCalledTimes(1);
    expect(constructors.grpc).toHaveBeenCalledWith();
    expect(constructors.json).toHaveBeenCalledWith(
      expect.objectContaining({url: 'https://secondary.example/v1/metrics'}),
    );
  });

  test('passes secondary compression and temporality to the exporter', () => {
    process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT =
      'http://primary.example/v1/metrics';
    process.env[secondaryEndpointEnv] = 'https://secondary.example/v1/metrics';
    process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_COMPRESSION = 'gzip';
    process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE =
      'delta';

    createMetricReadersFromEnv();

    expect(constructors.proto).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        compression: 'gzip',
        temporalityPreference: 0,
      }),
    );
  });

  test('uses the SDK fallback for an invalid primary protocol', () => {
    process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT =
      'http://primary.example/v1/metrics';
    process.env.OTEL_EXPORTER_OTLP_METRICS_PROTOCOL = 'invalid';
    process.env[secondaryEndpointEnv] = 'https://secondary.example/v1/metrics';

    createMetricReadersFromEnv();

    expect(constructors.proto).toHaveBeenCalledTimes(2);
    expect(constructors.proto).toHaveBeenNthCalledWith(1);
  });

  test('applies primary interval and timeout with SDK-compatible defaults', () => {
    process.env.OTEL_METRIC_EXPORT_INTERVAL = '15000';
    process.env.OTEL_METRIC_EXPORT_TIMEOUT = '20000';

    process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT =
      'http://primary.example/v1/metrics';
    process.env[secondaryEndpointEnv] = 'https://secondary.example/v1/metrics';

    createMetricReadersFromEnv();

    expect(constructors.reader).toHaveBeenCalledWith({
      exporter: expect.objectContaining({kind: 'proto'}),
      exportIntervalMillis: 15_000,
      exportTimeoutMillis: 15_000,
    });
  });

  test.each(['none', 'console', 'prometheus', 'otlp,console'])(
    'rejects unsupported primary exporter mode %s',
    exporter => {
      process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT =
        'http://primary.example/v1/metrics';
      process.env.OTEL_METRICS_EXPORTER = exporter;
      process.env[secondaryEndpointEnv] =
        'https://secondary.example/v1/metrics';

      expect(() => createMetricReadersFromEnv()).toThrow(
        'requires OTEL_METRICS_EXPORTER=otlp',
      );
    },
  );

  test('accepts a duplicate primary OTLP exporter like the SDK', () => {
    process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT =
      'http://primary.example/v1/metrics';
    process.env.OTEL_METRICS_EXPORTER = 'otlp,otlp';
    process.env[secondaryEndpointEnv] = 'https://secondary.example/v1/metrics';

    expect(createMetricReadersFromEnv()).toHaveLength(2);
  });

  test('requires an explicitly configured primary endpoint', () => {
    process.env.OTEL_METRICS_EXPORTER = 'otlp';
    process.env[secondaryEndpointEnv] = 'https://secondary.example/v1/metrics';

    expect(() => createMetricReadersFromEnv()).toThrow(
      'requires a primary OTEL_EXPORTER_OTLP_METRICS_ENDPOINT',
    );
  });

  test('isolates primary exporter environment from the secondary exporter', () => {
    const observed: NodeJS.ProcessEnv[] = [];
    constructors.proto.mockImplementation(function (options?: unknown) {
      observed.push({...process.env});
      return {kind: 'proto', options};
    });
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT = 'http://primary.example:4318';
    process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT =
      'http://primary.example/v1/metrics';
    process.env.OTEL_EXPORTER_OTLP_HEADERS = 'generic=primary';
    process.env.OTEL_EXPORTER_OTLP_METRICS_HEADERS = 'authorization=primary';
    process.env.OTEL_EXPORTER_OTLP_METRICS_COMPRESSION = 'gzip';
    process.env.OTEL_EXPORTER_OTLP_METRICS_TIMEOUT = '1234';
    process.env.OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE = '/primary/ca.pem';
    process.env.OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE = 'delta';
    process.env.OTEL_EXPORTER_OTLP_FUTURE_SETTING = 'primary';
    process.env[secondaryEndpointEnv] = 'https://secondary.example/v1/metrics';
    process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_HEADERS =
      'authorization=secondary';

    createMetricReadersFromEnv();

    expect(observed).toHaveLength(2);
    expect(observed[0].OTEL_EXPORTER_OTLP_METRICS_HEADERS).toBe(
      'authorization=primary',
    );
    expect(observed[0].SECONDARY_OTEL_EXPORTER_OTLP_METRICS_HEADERS).toBe(
      'authorization=secondary',
    );
    for (const [name, value] of Object.entries(observed[1])) {
      if (name.startsWith('OTEL_EXPORTER_OTLP')) {
        expect(value, name).toBeUndefined();
      }
    }
    expect(constructors.proto).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({
        headers: {authorization: 'secondary'},
        url: 'https://secondary.example/v1/metrics',
      }),
    );
    expect(process.env.OTEL_EXPORTER_OTLP_METRICS_HEADERS).toBe(
      'authorization=primary',
    );
    expect(process.env.OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE).toBe(
      '/primary/ca.pem',
    );
    expect(process.env.OTEL_EXPORTER_OTLP_FUTURE_SETTING).toBe('primary');
  });

  test('restores the primary environment when secondary construction throws', () => {
    process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT =
      'http://primary.example/v1/metrics';
    process.env.OTEL_EXPORTER_OTLP_METRICS_HEADERS = 'authorization=primary';
    process.env[secondaryEndpointEnv] = 'https://secondary.example/v1/metrics';
    constructors.proto
      .mockImplementationOnce(function () {
        return {kind: 'primary', options: undefined};
      })
      .mockImplementationOnce(function () {
        throw new Error('constructor failed');
      });

    expect(() => createMetricReadersFromEnv()).toThrow('constructor failed');
    expect(process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT).toBe(
      'http://primary.example/v1/metrics',
    );
    expect(process.env.OTEL_EXPORTER_OTLP_METRICS_HEADERS).toBe(
      'authorization=primary',
    );
  });

  test('passes gRPC secondary headers as metadata', () => {
    process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT =
      'http://primary.example:4317';
    process.env.OTEL_EXPORTER_OTLP_METRICS_PROTOCOL = 'grpc';
    process.env[secondaryEndpointEnv] = 'https://secondary.example:4317';
    process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_PROTOCOL = 'grpc';
    process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_HEADERS =
      'authorization=secondary';

    createMetricReadersFromEnv();

    const secondaryOptions = constructors.grpc.mock.calls[1][0] as {
      metadata: {getMap(): Record<string, unknown>};
    };
    expect(secondaryOptions.metadata.getMap()).toEqual({
      authorization: 'secondary',
    });
  });
});
