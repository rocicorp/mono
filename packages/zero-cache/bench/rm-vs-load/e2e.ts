/* oxlint-disable no-console */

const defaults = {
  // Pin the review scenario to the production-shaped pressure point: one RM
  // trying to hold 1k tx/s while sixteen VSs parse, apply, and acknowledge the
  // stream, with one VS rejoining from behind during the same load window.
  ZERO_RM_VS_FULL: '1',
  ZERO_RM_VS_DURATION_MS: '6000',
  ZERO_RM_VS_SUBSCRIBERS: '16',
  ZERO_RM_VS_SCENARIO: 'medium-wide-batch-pressure',
  ZERO_RM_VS_APPLY_MODE: 'worker-batch',
  ZERO_RM_VS_TRANSPORT: 'websocket',
  ZERO_RM_VS_CLIENT_CPU_US: '5',
  ZERO_RM_VS_SLOW_EVERY: '0',
  ZERO_RM_VS_RECONNECT_LAG_TX: '500',
  ZERO_RM_VS_FINAL_CATCHUP_TIMEOUT_MS: '15000',
  ZERO_RM_VS_SETTLE_MS: '50',
  ZERO_RM_VS_SMALL_TARGET_TPS: '5000',
  ZERO_RM_VS_MEDIUM_TARGET_TPS: '2000',
  ZERO_RM_VS_MEDIUM_WIDE_TARGET_TPS: '1000',
  ZERO_RM_VS_LARGE_TARGET_TPS: '600',
  ZERO_RM_VS_FLUSH_BYTES: '65536',
} as const;

for (const [name, value] of Object.entries(defaults)) {
  process.env[name] ??= value;
}

console.log(
  [
    'rm-vs-load e2e benchmark',
    '  rm: 1',
    `  view-syncers: ${process.env.ZERO_RM_VS_SUBSCRIBERS}`,
    `  duration-ms: ${process.env.ZERO_RM_VS_DURATION_MS}`,
    `  apply-mode: ${
      process.env.ZERO_RM_VS_APPLY_MODE ??
      (process.env.ZERO_RM_VS_APPLY_CLIENTS === '1' ? 'direct' : 'none')
    }`,
    `  consumer-runtime: ${process.env.ZERO_RM_VS_CONSUMER_RUNTIME ?? 'inline'}`,
    `  transport: ${process.env.ZERO_RM_VS_TRANSPORT ?? 'in-process'}`,
    `  protocol: ${process.env.ZERO_RM_VS_PROTOCOL ?? 'v7'}`,
    `  ws-ack: ${process.env.ZERO_RM_VS_WS_ACK ?? 'per-message'}`,
    `  ws-batch-messages: ${process.env.ZERO_RM_VS_WS_BATCH_MESSAGES ?? '64'}`,
    `  apply-clients: ${process.env.ZERO_RM_VS_APPLY_CLIENTS}`,
    `  client-cpu-us: ${process.env.ZERO_RM_VS_CLIENT_CPU_US}`,
    `  reconnect-lag-tx: ${process.env.ZERO_RM_VS_RECONNECT_LAG_TX}`,
    `  target-tps: small=${process.env.ZERO_RM_VS_SMALL_TARGET_TPS}, ` +
      `medium=${process.env.ZERO_RM_VS_MEDIUM_TARGET_TPS}, ` +
      `medium-wide=${process.env.ZERO_RM_VS_MEDIUM_WIDE_TARGET_TPS}, ` +
      `large=${process.env.ZERO_RM_VS_LARGE_TARGET_TPS}`,
    '',
    'flow:',
    `  RM -> Storer -> changeLog -> ${process.env.ZERO_RM_VS_SUBSCRIBERS} ViewSyncers`,
    '                  |',
    '                  +-> reconnect catchup when enabled',
  ].join('\n'),
);

await import('./index.ts');
