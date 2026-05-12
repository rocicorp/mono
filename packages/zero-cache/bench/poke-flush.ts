import {performance} from 'node:perf_hooks';
import {fileURLToPath} from 'node:url';
import type {Downstream} from '../../zero-protocol/src/down.ts';
import type {PokePartBody} from '../../zero-protocol/src/poke.ts';
import type {RowPatchOp} from '../../zero-protocol/src/row-patch.ts';
import {
  loadPayloadProfiles,
  smokePayloadProfiles,
  watermarkFor,
} from './load-fixtures.ts';
import {
  argValue,
  envFlag,
  envInt,
  formatBytes,
  formatRate,
  percentile,
  writeJsonSummary,
} from './perf-utils.ts';

type Strategy = 'legacy-fixed-100' | 'byte-aware-128kb-batched-preencoded';

type ScenarioResult = {
  readonly strategy: Strategy;
  readonly rowsPerTx: number;
  readonly payload: string;
  readonly payloadBytes: number;
  readonly targetTxPerSec: number;
  readonly tx: number;
  readonly rows: number;
  readonly messages: number;
  readonly pokeParts: number;
  readonly encodedBytes: number;
  readonly maxMessageBytes: number;
  readonly avgBytesPerMessage: number;
  readonly encodeCpuMs: number;
  readonly p95PokeLatencyMs: number;
  readonly p99PokeLatencyMs: number;
  readonly wallMs: number;
  readonly effectiveTxPerSec: number;
};

type Summary = {
  readonly name: 'zero-poke-flush-workload';
  readonly mode: 'smoke' | 'full';
  readonly generatedAt: string;
  readonly results: readonly ScenarioResult[];
};

const legacyFlushRows = 100;
const optimizedFlushRows = 100;
const optimizedFlushBytes = 128 * 1024;
const pokePartEnvelopeBytes = Buffer.byteLength('["pokePart",]');
const payloadCache = new Map<number, string>();

function payloadFor(bytes: number) {
  let payload = payloadCache.get(bytes);
  if (payload === undefined) {
    payload = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
      .repeat(Math.ceil(bytes / 62))
      .slice(0, bytes);
    payloadCache.set(bytes, payload);
  }
  return payload;
}

function makeRowPatch(
  tx: number,
  seq: number,
  payloadBytes: number,
): RowPatchOp {
  return {
    op: 'put',
    tableName: 'bench_rows',
    value: {
      id: `${tx.toString(36)}-${seq.toString(36)}`,
      tx,
      seq,
      bucket: seq % 32,
      payload: payloadFor(payloadBytes),
    },
  };
}

function estimatePokePartBaseBytes(pokeID: string) {
  return (
    pokePartEnvelopeBytes +
    Buffer.byteLength('{"pokeID":}') +
    Buffer.byteLength(JSON.stringify(pokeID))
  );
}

function estimateRowsPatchEntryBytes(row: RowPatchOp) {
  return (
    Buffer.byteLength(',"rowsPatch":') + Buffer.byteLength(JSON.stringify(row))
  );
}

function encode(msg: Downstream, preencoded: string | undefined) {
  const start = performance.now();
  const encoded = preencoded ?? JSON.stringify(msg);
  return {
    bytes: Buffer.byteLength(encoded),
    cpuMs: performance.now() - start,
  };
}

function runScenario(
  strategy: Strategy,
  rowsPerTx: number,
  payload: {readonly size: string; readonly bytes: number},
  tx: number,
  targetTxPerSec: number,
): ScenarioResult {
  let messages = 0;
  let pokeParts = 0;
  let encodedBytes = 0;
  let maxMessageBytes = 0;
  let encodeCpuMs = 0;
  const latencies: number[] = [];

  const send = (msg: Downstream, preencoded?: string) => {
    const result = encode(msg, preencoded);
    messages++;
    encodedBytes += result.bytes;
    maxMessageBytes = Math.max(maxMessageBytes, result.bytes);
    encodeCpuMs += result.cpuMs;
    if (msg[0] === 'pokePart') {
      pokeParts++;
    }
  };

  const start = performance.now();
  let baseCookie = watermarkFor(1);
  for (let i = 0; i < tx; i++) {
    const txStart = performance.now();
    const txNum = i + 2;
    const pokeID = watermarkFor(txNum);
    send(['pokeStart', {pokeID, baseCookie}]);

    let body: PokePartBody | undefined;
    let partCount = 0;
    let estimatedBytes = estimatePokePartBaseBytes(pokeID);
    const ensureBody = () => (body ??= {pokeID});
    const flush = () => {
      if (body === undefined) {
        return;
      }
      const msg: Downstream = ['pokePart', body];
      let preencoded: string | undefined;
      if (strategy === 'byte-aware-128kb-batched-preencoded') {
        const encodeStart = performance.now();
        preencoded = JSON.stringify(msg);
        encodeCpuMs += performance.now() - encodeStart;
      }
      send(msg, preencoded);
      body = undefined;
      partCount = 0;
      estimatedBytes = estimatePokePartBaseBytes(pokeID);
    };

    const append = (row: RowPatchOp) => {
      (ensureBody().rowsPatch ??= []).push(row);
      partCount++;
      estimatedBytes += estimateRowsPatchEntryBytes(row);
    };

    if (strategy === 'legacy-fixed-100') {
      for (let j = 0; j < rowsPerTx; j++) {
        append(makeRowPatch(txNum, j, payload.bytes));
        if (partCount >= legacyFlushRows) {
          flush();
        }
      }
    } else {
      for (let j = 0; j < rowsPerTx; j++) {
        append(makeRowPatch(txNum, j, payload.bytes));
        if (
          estimatedBytes >= optimizedFlushBytes ||
          partCount >= optimizedFlushRows
        ) {
          flush();
        }
      }
    }

    flush();
    send(['pokeEnd', {pokeID, cookie: pokeID}]);
    baseCookie = pokeID;
    latencies.push(performance.now() - txStart);
  }

  const wallMs = performance.now() - start;
  return {
    strategy,
    rowsPerTx,
    payload: payload.size,
    payloadBytes: payload.bytes,
    targetTxPerSec,
    tx,
    rows: tx * rowsPerTx,
    messages,
    pokeParts,
    encodedBytes,
    maxMessageBytes,
    avgBytesPerMessage: messages === 0 ? 0 : encodedBytes / messages,
    encodeCpuMs,
    p95PokeLatencyMs: percentile(latencies, 95),
    p99PokeLatencyMs: percentile(latencies, 99),
    wallMs,
    effectiveTxPerSec: (tx * 1000) / wallMs,
  };
}

function printResult(result: ScenarioResult) {
  console.log(
    [
      result.strategy,
      `${result.rowsPerTx} rows/tx`,
      `${result.payload} (${formatBytes(result.payloadBytes)})`,
      `${result.tx} tx @ ${formatRate(result.targetTxPerSec)} target tx/s`,
      `${result.messages} messages`,
      `${result.pokeParts} pokeParts`,
      `${formatBytes(result.encodedBytes)} encoded`,
      `${formatBytes(result.maxMessageBytes)} max-message`,
      `${result.encodeCpuMs.toFixed(2)} ms encode CPU`,
      `p95=${result.p95PokeLatencyMs.toFixed(3)} ms`,
      `p99=${result.p99PokeLatencyMs.toFixed(3)} ms`,
      `${formatRate(result.effectiveTxPerSec)} effective tx/s`,
    ].join(' | '),
  );
}

export async function main() {
  const full = envFlag('ZERO_POKE_FULL');
  const rowsVariants = full ? [10, 100] : [100];
  const payloads = full
    ? loadPayloadProfiles.filter(p => p.size !== 'medium')
    : smokePayloadProfiles;
  const transactions = envInt('ZERO_POKE_TX', full ? 1000 : 100);
  const targetTxPerSec = envInt('ZERO_POKE_TARGET_TX_PER_SEC', 1000);
  const output = argValue('out') ?? process.env.ZERO_BENCH_OUT;
  const results: ScenarioResult[] = [];

  for (const rows of rowsVariants) {
    for (const payload of payloads) {
      for (const strategy of [
        'legacy-fixed-100',
        'byte-aware-128kb-batched-preencoded',
      ] satisfies Strategy[]) {
        const result = runScenario(
          strategy,
          rows,
          payload,
          transactions,
          targetTxPerSec,
        );
        results.push(result);
        printResult(result);
      }
    }
  }

  const summary: Summary = {
    name: 'zero-poke-flush-workload',
    mode: full ? 'full' : 'smoke',
    generatedAt: new Date().toISOString(),
    results,
  };
  await writeJsonSummary(summary, output);
  console.log(JSON.stringify(summary));
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await main();
}
