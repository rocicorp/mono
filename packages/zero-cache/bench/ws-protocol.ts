import {performance} from 'node:perf_hooks';
import {fileURLToPath} from 'node:url';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {Downstream} from '../../zero-protocol/src/down.ts';
import type {PokePartMessage} from '../../zero-protocol/src/poke.ts';
import type {RowPatchOp} from '../../zero-protocol/src/row-patch.ts';
import {loadPayloadProfiles} from './load-fixtures.ts';
import {
  argValue,
  envFlag,
  envInt,
  formatBytes,
  formatRate,
  writeJsonSummary,
} from './perf-utils.ts';

type RowPatchPut = Extract<RowPatchOp, {op: 'put'}>;
type RowValue = Record<string, ReadonlyJSONValue | undefined>;
type CodecName =
  | 'json-text-pokePart'
  | 'binary-frame-utf8-json'
  | 'dict-json-row-patch'
  | 'custom-binary-row-patch';

type Codec = {
  readonly name: CodecName;
  readonly encode: (msg: PokePartMessage) => string | Buffer;
  readonly decode: (encoded: string | Buffer) => Downstream;
};

type CodecResult = {
  readonly codec: CodecName;
  readonly rows: number;
  readonly payload: string;
  readonly payloadBytes: number;
  readonly encodedBytes: number;
  readonly iterations: number;
  readonly encodeDecodeMs: number;
  readonly opsPerSec: number;
  readonly decodedRows: number;
};

type CodecComparison = {
  readonly rows: number;
  readonly payload: string;
  readonly payloadBytes: number;
  readonly baselineCodec: 'dict-json-row-patch';
  readonly candidateCodec: 'custom-binary-row-patch';
  readonly baselineEncodedBytes: number;
  readonly candidateEncodedBytes: number;
  readonly encodedByteReductionPct: number;
  readonly baselineEncodeDecodeMs: number;
  readonly candidateEncodeDecodeMs: number;
  readonly encodeDecodeReductionPct: number;
  readonly baselineOpsPerSec: number;
  readonly candidateOpsPerSec: number;
  readonly candidateDecodedRows: number;
  readonly representative: boolean;
  readonly meetsStack5Threshold: boolean;
};

type Recommendation = {
  readonly decision:
    | 'custom-binary-stack-5-candidate'
    | 'dictionary-json-or-no-production-protocol-change'
    | 'representative-cases-not-run';
  readonly threshold: string;
  readonly representativeCases: number;
  readonly passingRepresentativeCases: number;
  readonly text: string;
};

type Summary = {
  readonly name: 'zero-ws-protocol-microbench';
  readonly mode: 'smoke' | 'full';
  readonly generatedAt: string;
  readonly results: readonly CodecResult[];
  readonly comparisons: readonly CodecComparison[];
  readonly recommendation: Recommendation;
  readonly skipped: readonly string[];
};

const stack5ThresholdPct = 25;
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const columns = [
  'id',
  'title',
  'status',
  'ownerID',
  'updatedAt',
  'payload',
] as const;
const columnCodes = Object.fromEntries(columns.map((col, i) => [col, i]));
const tableCodes = {issue: 0} as const;
const reverseTables = ['issue'] as const;
const reverseColumns = columns;

function makePayload(bytes: number): string {
  return 'x'.repeat(bytes);
}

function makePokePart(rows: number, payloadBytes: number): PokePartMessage {
  const payload = makePayload(payloadBytes);
  const rowsPatch: RowPatchPut[] = Array.from({length: rows}, (_, i) => ({
    op: 'put',
    tableName: 'issue',
    value: {
      id: `issue-${i}`,
      title: `Issue ${i}`,
      status: i % 2 === 0 ? 'open' : 'closed',
      ownerID: `user-${i % 10}`,
      updatedAt: 1_800_000_000_000 + i,
      payload,
    },
  }));
  return [
    'pokePart',
    {
      pokeID: 'bench-poke',
      lastMutationIDChanges: {'client-1': rows},
      gotQueriesPatch: [{op: 'put', hash: 'query-hash', ttl: 300_000}],
      rowsPatch,
    },
  ];
}

function rowCount(msg: Downstream): number {
  if (msg[0] !== 'pokePart') {
    return 0;
  }
  return msg[1].rowsPatch?.length ?? 0;
}

const jsonTextCodec: Codec = {
  name: 'json-text-pokePart',
  encode: msg => JSON.stringify(msg),
  decode: encoded => JSON.parse(encoded.toString()) as Downstream,
};

const binaryUtf8JsonCodec: Codec = {
  name: 'binary-frame-utf8-json',
  encode: msg => Buffer.from(JSON.stringify(msg), 'utf8'),
  decode: encoded => JSON.parse(encoded.toString()) as Downstream,
};

type DictPut = readonly [0, 0, readonly unknown[]];
type DictMessage = readonly [
  'pokePartDict',
  {
    readonly pokeID: string;
    readonly d: {
      readonly t: readonly string[];
      readonly c: readonly string[];
    };
    readonly rowsPatch: readonly DictPut[];
  },
];

function encodeDict(msg: PokePartMessage): string {
  const body = msg[1];
  const patch = body.rowsPatch ?? [];
  const dict: DictMessage = [
    'pokePartDict',
    {
      pokeID: body.pokeID,
      d: {t: ['issue'], c: columns},
      rowsPatch: patch.map(op => {
        if (op.op !== 'put') {
          throw new Error(`dict codec only supports put ops in this bench`);
        }
        return [
          tableCodes[op.tableName as keyof typeof tableCodes],
          0,
          columns.map(col => op.value[col]),
        ];
      }),
    },
  ];
  return JSON.stringify(dict);
}

function decodeDict(encoded: string | Buffer): PokePartMessage {
  const msg = JSON.parse(encoded.toString()) as DictMessage;
  return [
    'pokePart',
    {
      pokeID: msg[1].pokeID,
      rowsPatch: msg[1].rowsPatch.map(([tableCode, opCode, values]) => {
        if (opCode !== 0) {
          throw new Error(`Unsupported dict row op ${opCode}`);
        }
        const value: RowValue = Object.fromEntries(
          msg[1].d.c.map((col, i) => [col, values[i] as ReadonlyJSONValue]),
        );
        return {
          op: 'put',
          tableName: msg[1].d.t[tableCode] ?? 'unknown',
          value,
        } satisfies RowPatchPut;
      }),
    },
  ];
}

const dictCodec: Codec = {
  name: 'dict-json-row-patch',
  encode: encodeDict,
  decode: decodeDict,
};

function byteLength(value: string): number {
  return textEncoder.encode(value).byteLength;
}

function writeString(
  bytes: Uint8Array,
  view: DataView,
  offset: number,
  value: string,
) {
  const encoded = textEncoder.encode(value);
  view.setUint16(offset, encoded.length);
  offset += 2;
  bytes.set(encoded, offset);
  return offset + encoded.length;
}

function readString(bytes: Uint8Array, view: DataView, offset: number) {
  const length = view.getUint16(offset);
  offset += 2;
  return {
    value: textDecoder.decode(bytes.subarray(offset, offset + length)),
    offset: offset + length,
  };
}

function customBinarySize(msg: PokePartMessage): number {
  let bytes = 1 + 2;
  for (const op of msg[1].rowsPatch ?? []) {
    if (op.op !== 'put') {
      throw new Error(
        `custom binary codec only supports put ops in this bench`,
      );
    }
    bytes += 1;
    for (const col of columns) {
      const value = op.value[col];
      bytes += 1;
      if (typeof value === 'number') {
        bytes += 8;
      } else {
        bytes += 2 + byteLength(String(value));
      }
    }
  }
  return bytes;
}

function encodeCustomBinary(msg: PokePartMessage): Buffer {
  const rows = msg[1].rowsPatch ?? [];
  const bytes = new Uint8Array(customBinarySize(msg));
  const view = new DataView(bytes.buffer);
  let offset = 0;
  view.setUint8(offset, 1);
  offset += 1;
  view.setUint16(offset, rows.length);
  offset += 2;

  for (const op of rows) {
    if (op.op !== 'put') {
      throw new Error(
        `custom binary codec only supports put ops in this bench`,
      );
    }
    view.setUint8(offset, tableCodes[op.tableName as keyof typeof tableCodes]);
    offset += 1;
    for (const col of columns) {
      const value = op.value[col];
      const code = columnCodes[col];
      if (typeof value === 'number') {
        view.setUint8(offset, 0x80 | code);
        offset += 1;
        view.setFloat64(offset, value);
        offset += 8;
      } else {
        view.setUint8(offset, code);
        offset += 1;
        offset = writeString(bytes, view, offset, String(value));
      }
    }
  }
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength);
}

function decodeCustomBinary(encoded: string | Buffer): PokePartMessage {
  const bytes =
    typeof encoded === 'string'
      ? textEncoder.encode(encoded)
      : new Uint8Array(encoded.buffer, encoded.byteOffset, encoded.byteLength);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let offset = 0;
  const version = view.getUint8(offset);
  offset += 1;
  if (version !== 1) {
    throw new Error(`Unsupported custom row patch version ${version}`);
  }
  const rowCount = view.getUint16(offset);
  offset += 2;
  const rowsPatch: RowPatchPut[] = [];

  for (let i = 0; i < rowCount; i++) {
    const tableName = reverseTables[view.getUint8(offset)] ?? 'unknown';
    offset += 1;
    const value: RowValue = {};
    for (let j = 0; j < columns.length; j++) {
      const tag = view.getUint8(offset);
      offset += 1;
      const numeric = (tag & 0x80) !== 0;
      const column = reverseColumns[tag & 0x7f];
      if (column === undefined) {
        throw new Error(`Unknown custom row patch column ${tag & 0x7f}`);
      }
      if (numeric) {
        value[column] = view.getFloat64(offset);
        offset += 8;
      } else {
        const decoded = readString(bytes, view, offset);
        value[column] = decoded.value;
        offset = decoded.offset;
      }
    }
    rowsPatch.push({op: 'put', tableName, value});
  }

  return ['pokePart', {pokeID: 'bench-poke', rowsPatch}];
}

const customBinaryCodec: Codec = {
  name: 'custom-binary-row-patch',
  encode: encodeCustomBinary,
  decode: decodeCustomBinary,
};

const codecs = [
  jsonTextCodec,
  dictCodec,
  binaryUtf8JsonCodec,
  customBinaryCodec,
];

function runCodec(
  codec: Codec,
  msg: PokePartMessage,
  rows: number,
  payload: string,
  payloadBytes: number,
  iterations: number,
): CodecResult {
  const sample = codec.encode(msg);
  const encodedBytes = Buffer.byteLength(sample);
  let decodedRows = 0;
  const start = performance.now();
  for (let i = 0; i < iterations; i++) {
    decodedRows += rowCount(codec.decode(codec.encode(msg)));
  }
  const encodeDecodeMs = performance.now() - start;
  return {
    codec: codec.name,
    rows,
    payload,
    payloadBytes,
    encodedBytes,
    iterations,
    encodeDecodeMs,
    opsPerSec: (iterations * 1000) / encodeDecodeMs,
    decodedRows,
  };
}

function printResult(result: CodecResult) {
  console.log(
    [
      `${result.codec}`,
      `rows=${result.rows}`,
      `payload=${result.payload} (${formatBytes(result.payloadBytes)})`,
      `encoded=${formatBytes(result.encodedBytes)}`,
      `encodeDecode=${result.encodeDecodeMs.toFixed(2)} ms`,
      `ops=${formatRate(result.opsPerSec)}/s`,
      `decodedRows=${result.decodedRows}`,
    ].join(' | '),
  );
}

function pctReduction(before: number, after: number): number {
  if (before === 0) {
    return 0;
  }
  return ((before - after) / before) * 100;
}

function isRepresentativeCase(result: CodecResult): boolean {
  return result.rows >= 100 || result.payload === 'large';
}

function findResult(
  results: readonly CodecResult[],
  codec: CodecName,
): CodecResult {
  const result = results.find(result => result.codec === codec);
  if (result === undefined) {
    throw new Error(`Missing codec result for ${codec}`);
  }
  return result;
}

function compareCustomToDict(results: readonly CodecResult[]): CodecComparison {
  const baseline = findResult(results, 'dict-json-row-patch');
  const candidate = findResult(results, 'custom-binary-row-patch');
  const encodedByteReductionPct = pctReduction(
    baseline.encodedBytes,
    candidate.encodedBytes,
  );
  const encodeDecodeReductionPct = pctReduction(
    baseline.encodeDecodeMs,
    candidate.encodeDecodeMs,
  );

  return {
    rows: baseline.rows,
    payload: baseline.payload,
    payloadBytes: baseline.payloadBytes,
    baselineCodec: 'dict-json-row-patch',
    candidateCodec: 'custom-binary-row-patch',
    baselineEncodedBytes: baseline.encodedBytes,
    candidateEncodedBytes: candidate.encodedBytes,
    encodedByteReductionPct,
    baselineEncodeDecodeMs: baseline.encodeDecodeMs,
    candidateEncodeDecodeMs: candidate.encodeDecodeMs,
    encodeDecodeReductionPct,
    baselineOpsPerSec: baseline.opsPerSec,
    candidateOpsPerSec: candidate.opsPerSec,
    candidateDecodedRows: candidate.decodedRows,
    representative: isRepresentativeCase(baseline),
    meetsStack5Threshold:
      encodedByteReductionPct >= stack5ThresholdPct ||
      encodeDecodeReductionPct >= stack5ThresholdPct,
  };
}

function formatPct(value: number): string {
  return `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`;
}

function printComparison(comparison: CodecComparison) {
  console.log(
    [
      `${comparison.candidateCodec} vs ${comparison.baselineCodec}`,
      `rows=${comparison.rows}`,
      `payload=${comparison.payload} (${formatBytes(comparison.payloadBytes)})`,
      `bytes=${formatBytes(comparison.candidateEncodedBytes)} vs ${formatBytes(
        comparison.baselineEncodedBytes,
      )}`,
      `byteReduction=${formatPct(comparison.encodedByteReductionPct)}`,
      `encodeDecode=${comparison.candidateEncodeDecodeMs.toFixed(
        2,
      )} ms vs ${comparison.baselineEncodeDecodeMs.toFixed(2)} ms`,
      `timeReduction=${formatPct(comparison.encodeDecodeReductionPct)}`,
      `representative=${comparison.representative}`,
      `stack5Threshold=${comparison.meetsStack5Threshold}`,
    ].join(' | '),
  );
}

function makeRecommendation(
  comparisons: readonly CodecComparison[],
): Recommendation {
  const threshold = `custom binary must beat dictionary JSON by >=${stack5ThresholdPct}% encode+decode time or >=${stack5ThresholdPct}% bytes on every representative large/fanout case`;
  const representative = comparisons.filter(
    comparison => comparison.representative,
  );
  const passing = representative.filter(
    comparison => comparison.meetsStack5Threshold,
  );

  if (representative.length === 0) {
    return {
      decision: 'representative-cases-not-run',
      threshold,
      representativeCases: 0,
      passingRepresentativeCases: 0,
      text: 'NO-GO for Stack 5 custom binary from this run: representative large/fanout cases were not run; keep production protocol unchanged and run perf:ws:full before reconsidering.',
    };
  }

  if (passing.length === representative.length) {
    return {
      decision: 'custom-binary-stack-5-candidate',
      threshold,
      representativeCases: representative.length,
      passingRepresentativeCases: passing.length,
      text: `GO for Stack 5 custom binary candidate: ${passing.length}/${representative.length} representative large/fanout cases beat dictionary JSON by >=${stack5ThresholdPct}% on encode+decode time or bytes.`,
    };
  }

  return {
    decision: 'dictionary-json-or-no-production-protocol-change',
    threshold,
    representativeCases: representative.length,
    passingRepresentativeCases: passing.length,
    text: `NO-GO for Stack 5 custom binary: ${passing.length}/${representative.length} representative large/fanout cases beat dictionary JSON by >=${stack5ThresholdPct}% on encode+decode time or bytes; recommend dictionary JSON as the comparison baseline or no production protocol change.`,
  };
}

export async function main() {
  const full = envFlag('ZERO_WS_FULL');
  const rowsVariants = full ? [1, 10, 100] : [10, 100];
  const payloads = full
    ? loadPayloadProfiles
    : loadPayloadProfiles.filter(
        payload => payload.size === 'small' || payload.size === 'large',
      );
  const iterations = envInt('ZERO_WS_ITERATIONS', full ? 200 : 100);
  const output = argValue('out') ?? process.env.ZERO_BENCH_OUT;
  const results: CodecResult[] = [];
  const comparisons: CodecComparison[] = [];

  for (const rows of rowsVariants) {
    for (const payload of payloads) {
      const msg = makePokePart(rows, payload.bytes);
      const scenarioResults: CodecResult[] = [];
      for (const codec of codecs) {
        const result = runCodec(
          codec,
          msg,
          rows,
          payload.size,
          payload.bytes,
          iterations,
        );
        results.push(result);
        scenarioResults.push(result);
        printResult(result);
      }
      const comparison = compareCustomToDict(scenarioResults);
      comparisons.push(comparison);
      printComparison(comparison);
    }
  }
  const recommendation = makeRecommendation(comparisons);

  const summary: Summary = {
    name: 'zero-ws-protocol-microbench',
    mode: full ? 'full' : 'smoke',
    generatedAt: new Date().toISOString(),
    results,
    comparisons,
    recommendation,
    skipped: [
      'MessagePack: not run because zero-cache has no MessagePack dependency; no dependency was added for this benchmark.',
      'CBOR: not run because zero-cache has no CBOR dependency; no dependency was added for this benchmark.',
    ],
  };
  await writeJsonSummary(summary, output);
  console.log(recommendation.text);
  console.log(JSON.stringify(summary));
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await main();
}
