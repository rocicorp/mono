import {createServer, type IncomingHttpHeaders, type Server} from 'node:http';
import type {AddressInfo} from 'node:net';
import {MeterProvider} from '@opentelemetry/sdk-metrics';
import {afterEach, beforeEach, expect, test} from 'vitest';
import {createMetricReadersFromEnv} from './otel-metric-readers.ts';

const originalEnv = {...process.env};
const servers: Server[] = [];

beforeEach(() => {
  process.env = {...originalEnv};
  for (const name of Object.keys(process.env)) {
    if (name.startsWith('OTEL_') || name.startsWith('SECONDARY_OTEL_')) {
      delete process.env[name];
    }
  }
});

afterEach(async () => {
  process.env = {...originalEnv};
  await Promise.all(
    servers.splice(0).map(
      server =>
        new Promise<void>((resolve, reject) => {
          server.close(error => (error ? reject(error) : resolve()));
        }),
    ),
  );
});

test('a failing secondary exporter does not prevent a primary export', async () => {
  const primaryHeaders: IncomingHttpHeaders[] = [];
  const secondaryHeaders: IncomingHttpHeaders[] = [];
  const primary = await listen((headers, respond) => {
    primaryHeaders.push(headers);
    respond(200);
  });
  const secondary = await listen((headers, respond) => {
    secondaryHeaders.push(headers);
    respond(400);
  });

  process.env.OTEL_METRICS_EXPORTER = 'otlp';
  process.env.OTEL_EXPORTER_OTLP_METRICS_PROTOCOL = 'http/protobuf';
  process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT = primary;
  process.env.OTEL_EXPORTER_OTLP_METRICS_HEADERS = 'authorization=primary';
  process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_ENDPOINT = secondary;
  process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_PROTOCOL = 'http/protobuf';
  process.env.SECONDARY_OTEL_EXPORTER_OTLP_METRICS_HEADERS =
    'authorization=secondary';

  const readers = createMetricReadersFromEnv();
  expect(readers).toHaveLength(2);
  if (!readers) {
    throw new Error('Expected secondary metric readers');
  }
  const provider = new MeterProvider({readers});
  try {
    provider
      .getMeter('failure-isolation-test')
      .createCounter('requests')
      .add(1);

    await provider.forceFlush().catch(() => {
      // The secondary response is intentionally an export failure. The primary
      // reader must still run and deliver its independent request.
    });

    expect(primaryHeaders).toHaveLength(1);
    expect(primaryHeaders[0].authorization).toBe('primary');
    expect(secondaryHeaders.length).toBeGreaterThanOrEqual(1);
    expect(secondaryHeaders[0].authorization).toBe('secondary');
  } finally {
    await provider.shutdown().catch(() => {});
  }
});

async function listen(
  onRequest: (
    headers: IncomingHttpHeaders,
    respond: (status: number) => void,
  ) => void,
): Promise<string> {
  const server = createServer((request, response) => {
    request.resume();
    request.on('end', () => {
      onRequest(request.headers, status => {
        response.writeHead(status, {'content-type': 'application/x-protobuf'});
        response.end();
      });
    });
  });
  servers.push(server);
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address() as AddressInfo;
  return `http://127.0.0.1:${address.port}/v1/metrics`;
}
