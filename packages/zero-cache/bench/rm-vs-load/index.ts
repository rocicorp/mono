/* oxlint-disable no-console */
import {BenchmarkConfigLoader, EnvReader} from './config.ts';
import {formatRate, writeJsonSummary} from './perf-utils.ts';
import {
  BenchmarkSuite,
  formatOperationCounts,
  formatReconnectSummary,
} from './runner.ts';
import {describeScenarios, loadScenarios} from './scenarios.ts';

const env = new EnvReader();
const config = new BenchmarkConfigLoader(env).load();
const scenarios = loadScenarios({full: config.full, env});

console.log(`scenario bytes: ${describeScenarios(scenarios)}`);

const summary = await new BenchmarkSuite(config, scenarios).run();

for (const result of summary.scenarios) {
  console.log(
    `${result.name}: ${formatRate(result.writeLoopTxPerSec)} tx/s load | ` +
      `${formatRate(result.writeLoopRowsPerSec)} rows/s load | ` +
      `ops ${formatOperationCounts(result.operationCounts)} | ` +
      `${formatRate(result.fanoutMessagesPerSec)} fanout msg/s | ` +
      `p95 ${result.p95TxLatencyMs.toFixed(3)} ms | ` +
      `vs-tx ${result.avgSubscriberTxApplyMs.toFixed(3)} ms | ` +
      formatSyncWorkerSummary(result) +
      `drain ${result.storerDrainMs.toFixed(1)} ms | ` +
      `max lag ${result.maxAckLagMessages}` +
      formatReconnectSummary(result),
  );
}

console.log(JSON.stringify(summary));
await writeJsonSummary(summary, config.outputPath);

function formatSyncWorkerSummary(result: {
  readonly syncWorkerCount: number;
  readonly syncWorkerReadsPerSec: number;
  readonly avgSyncWorkerReadMs: number;
  readonly maxSyncWorkerReadMs: number;
  readonly syncWorkerErrors: number;
}) {
  if (result.syncWorkerCount === 0) {
    return '';
  }
  return (
    `sync-workers ${result.syncWorkerCount} ` +
    `${formatRate(result.syncWorkerReadsPerSec)} reads/s ` +
    `avg ${result.avgSyncWorkerReadMs.toFixed(3)} ms ` +
    `max ${result.maxSyncWorkerReadMs.toFixed(3)} ms ` +
    `errors ${result.syncWorkerErrors} | `
  );
}
