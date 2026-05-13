/* oxlint-disable no-console */

const defaults = {
  // #5976/#5977 pin the review scenario here so future storer perf PRs compare
  // against the same 1 RM / 16 view-syncer load instead of tuning a new local
  // workload for each hypothesis.
  ZERO_RM_VS_FULL: '1',
  ZERO_RM_VS_DURATION_MS: '1200',
  ZERO_RM_VS_SUBSCRIBERS: '16',
  ZERO_RM_VS_SLOW_EVERY: '0',
  ZERO_RM_VS_RECONNECT_LAG_TX: '999999',
  ZERO_RM_VS_SETTLE_MS: '50',
  ZERO_RM_VS_SMALL_TARGET_TPS: '5000',
  ZERO_RM_VS_MEDIUM_TARGET_TPS: '2000',
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
    `  target-tps: small=${process.env.ZERO_RM_VS_SMALL_TARGET_TPS}, ` +
      `medium=${process.env.ZERO_RM_VS_MEDIUM_TARGET_TPS}, ` +
      `large=${process.env.ZERO_RM_VS_LARGE_TARGET_TPS}`,
    '',
    'flow:',
    '  RM -> Storer -> changeLog -> 16 ViewSyncers',
    '                  |',
    '                  +-> reconnect catchup when enabled',
  ].join('\n'),
);

await import('./index.ts');
