import {expect, test} from 'vitest';
import {
  BenchmarkConfigLoader,
  EnvReader,
  protocolVersionForMode,
} from './config.ts';

function loadConfig(
  env: Record<string, string | undefined>,
  argv: readonly string[] = ['node', 'bench'],
) {
  return new BenchmarkConfigLoader(new EnvReader(env, argv)).load();
}

test('loads smoke defaults from an explicit environment', () => {
  const config = loadConfig({});

  expect(config.mode).toBe('smoke');
  expect(config.durationMs).toBe(1_000);
  expect(config.settleMs).toBe(100);
  expect(config.consumer.count).toBe(4);
  expect(config.consumer.applyMode).toBe('none');
  expect(config.consumer.transportMode).toBe('in-process');
  expect(config.consumer.transportAckMode).toBe('per-message');
  expect(config.consumer.protocolMode).toBe('v6');
  expect(config.outputPath).toBeUndefined();
});

test('loads review shaped full config without touching process.env', () => {
  const config = loadConfig(
    {
      ZERO_RM_VS_FULL: '1',
      ZERO_RM_VS_SUBSCRIBERS: '1',
      ZERO_RM_VS_APPLY_MODE: 'direct',
      ZERO_RM_VS_APPLY_LIMIT: '1',
      ZERO_RM_VS_CONSUMER_RUNTIME: 'worker',
      ZERO_RM_VS_TRANSPORT: 'websocket',
      ZERO_RM_VS_WS_ACK: 'cumulative',
      ZERO_RM_VS_WS_BATCH_MESSAGES: '256',
      ZERO_RM_VS_SOURCE_APPLY: '1',
      ZERO_RM_VS_PG_IMAGE: 'postgres:18',
    },
    ['node', 'bench', '--out', 'summary.json'],
  );

  expect(config.mode).toBe('full');
  expect(config.consumer.count).toBe(1);
  expect(config.consumer.applyMode).toBe('direct');
  expect(config.consumer.applyLimit).toBe(1);
  expect(config.consumer.runtime).toBe('worker');
  expect(config.consumer.transportMode).toBe('websocket');
  expect(config.consumer.transportAckMode).toBe('cumulative');
  expect(config.consumer.transportBatchMessages).toBe(256);
  expect(config.sourceApply).toBe(true);
  expect(config.pgImage).toBe('postgres:18');
  expect(config.outputPath).toBe('summary.json');
});

test('preserves legacy apply client flag as direct apply mode', () => {
  const config = loadConfig({ZERO_RM_VS_APPLY_CLIENTS: '1'});

  expect(config.consumer.applyMode).toBe('direct');
  expect(config.consumer.applyMessages).toBe(true);
});

test('rejects invalid enum values at config load time', () => {
  expect(() => loadConfig({ZERO_RM_VS_TRANSPORT: 'udp'})).toThrow(
    /Invalid ZERO_RM_VS_TRANSPORT=udp/,
  );
  expect(() => loadConfig({ZERO_RM_VS_WS_ACK: 'latest'})).toThrow(
    /Invalid ZERO_RM_VS_WS_ACK=latest/,
  );
  expect(() => loadConfig({ZERO_RM_VS_PROTOCOL: 'v7'})).toThrow(
    /Invalid ZERO_RM_VS_PROTOCOL=v7/,
  );
});

test('maps protocol mode to the active stream protocol version', () => {
  expect(protocolVersionForMode('v6')).toBe(6);
});
