import {describe, test} from 'vitest';
import {createManualBenchmarkRecorder} from '../../../shared/src/bench.ts';
import {
  type BinaryCopyContentSummary,
  summarizeBinaryCopyFields,
  validateBinaryCopyContent,
} from './pg-copy-bench-validation.ts';
import {BinaryCopyParser} from './pg-copy-binary.ts';

const PROFILE_ENV = 'ZERO_PG_COPY_BENCH_PROFILE';
const BYTES_PER_MB = 1_000_000;
const MIN_SAMPLES = 50;
const MIN_MEASURED_MS = 2_000;

type ParserCase = {
  name: string;
  fieldBytes: number;
  chunkBytes: number;
  columns: number;
  rows: number;
  warmupReps: number;
};

const CASES = {
  'contained-4k-31k': {
    name: 'contained-4k-31k',
    fieldBytes: 4_096,
    chunkBytes: 31_744,
    columns: 7,
    rows: 4_096,
    warmupReps: 2,
  },
  'large-payload-270k-5.5k': {
    name: 'large-payload-270k-5.5k',
    fieldBytes: 276_480,
    chunkBytes: 5_632,
    columns: 7,
    rows: 64,
    warmupReps: 1,
  },
  'wide-text-683k-31k': {
    name: 'wide-text-683k-31k',
    fieldBytes: 699_392,
    chunkBytes: 31_744,
    columns: 25,
    rows: 32,
    warmupReps: 1,
  },
  'wide-text-683k-5.5k': {
    name: 'wide-text-683k-5.5k',
    fieldBytes: 699_392,
    chunkBytes: 5_632,
    columns: 25,
    rows: 16,
    warmupReps: 1,
  },
} as const satisfies Record<string, ParserCase>;

const profile = process.env[PROFILE_ENV] ?? 'default';
if (profile !== 'default' && profile !== 'wide-text') {
  throw new Error(
    `${PROFILE_ENV} must be one of default, wide-text; got ${JSON.stringify(profile)}`,
  );
}
const selectedCases =
  profile === 'default'
    ? [CASES['contained-4k-31k'], CASES['large-payload-270k-5.5k']]
    : [CASES['wide-text-683k-31k'], CASES['wide-text-683k-5.5k']];

const fixtures = selectedCases.map(benchmarkCase => ({
  benchmarkCase,
  fixture: buildFixture(benchmarkCase),
}));
const benchmarkRecorder = createManualBenchmarkRecorder();

describe(`pg-copy/BinaryCopyParser throughput (${profile})`, () => {
  test.each(fixtures)('$benchmarkCase.name', ({benchmarkCase, fixture}) => {
    validateBinaryCopyContent(fixture.chunks, fixture.expectedContent);
    for (let rep = 0; rep < benchmarkCase.warmupReps; rep++) {
      assertResult(fixture, consumeFixture(fixture));
    }

    const samples: number[] = [];
    let measuredMs = 0;
    while (samples.length < MIN_SAMPLES || measuredMs < MIN_MEASURED_MS) {
      const start = performance.now();
      const result = consumeFixture(fixture);
      const elapsed = performance.now() - start;
      assertResult(fixture, result);
      samples.push(elapsed);
      measuredMs += elapsed;
    }

    benchmarkRecorder.recordThroughput(
      `pg-copy/BinaryCopyParser ${benchmarkCase.name} binary stream MB ` +
        `[min 50 samples, 2s measured]`,
      samples,
      fixture.streamBytes / BYTES_PER_MB,
    );
  });
});

type ParserFixture = {
  chunks: readonly Buffer[];
  streamBytes: number;
  expectedFields: number;
  expectedPayloadBytes: number;
  expectedChecksum: number;
  expectedContent: BinaryCopyContentSummary;
};

type ParserResult = {
  fields: number;
  payloadBytes: number;
  checksum: number;
};

function buildFixture({
  fieldBytes,
  chunkBytes,
  columns,
  rows,
}: ParserCase): ParserFixture {
  const header = Buffer.from([
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0, 0, 0,
    0, 0, 0, 0, 0,
  ]);
  const trailer = Buffer.alloc(2);
  trailer.writeInt16BE(-1);
  const largeField = patternedBuffer(fieldBytes, 17);
  const smallField = patternedBuffer(16, 29);
  const fields = [largeField, ...Array<Buffer>(columns - 1).fill(smallField)];
  const row = tuple(fields);
  const stream = Buffer.concat([
    header,
    ...Array<Buffer>(rows).fill(row),
    trailer,
  ]);
  const chunks: Buffer[] = [];
  for (let offset = 0; offset < stream.length; offset += chunkBytes) {
    chunks.push(stream.subarray(offset, offset + chunkBytes));
  }

  let rowChecksum = 0;
  for (const field of fields) {
    rowChecksum = fieldChecksum(rowChecksum, field);
  }
  let expectedChecksum = 0;
  for (let rowNumber = 0; rowNumber < rows; rowNumber++) {
    expectedChecksum = (expectedChecksum + rowChecksum) >>> 0;
  }
  function* expectedFields() {
    for (let rowNumber = 0; rowNumber < rows; rowNumber++) {
      yield* fields;
    }
  }

  return {
    chunks,
    streamBytes: stream.length,
    expectedFields: columns * rows,
    expectedPayloadBytes: rows * (fieldBytes + (columns - 1) * 16),
    expectedChecksum,
    expectedContent: summarizeBinaryCopyFields(expectedFields()),
  };
}

function consumeFixture(fixture: ParserFixture): ParserResult {
  const parser = new BinaryCopyParser();
  let fields = 0;
  let payloadBytes = 0;
  let checksum = 0;

  for (const chunk of fixture.chunks) {
    for (const field of parser.parse(chunk)) {
      fields++;
      if (field !== null) {
        payloadBytes += field.length;
        checksum = fieldChecksum(checksum, field);
      }
    }
  }
  return {fields, payloadBytes, checksum};
}

function assertResult(fixture: ParserFixture, result: ParserResult) {
  if (
    result.fields !== fixture.expectedFields ||
    result.payloadBytes !== fixture.expectedPayloadBytes ||
    result.checksum !== fixture.expectedChecksum
  ) {
    throw new Error(
      `invalid parser result: fields=${result.fields}/${fixture.expectedFields}, ` +
        `payloadBytes=${result.payloadBytes}/${fixture.expectedPayloadBytes}, ` +
        `checksum=${result.checksum}/${fixture.expectedChecksum}`,
    );
  }
}

function fieldChecksum(checksum: number, field: Buffer) {
  return (
    (checksum + field.length + (field[0] ?? 0) + (field.at(-1) ?? 0)) >>> 0
  );
}

function tuple(fields: readonly Buffer[]) {
  const count = Buffer.alloc(2);
  count.writeInt16BE(fields.length);
  const parts: Buffer[] = [count];
  for (const field of fields) {
    const length = Buffer.alloc(4);
    length.writeInt32BE(field.length);
    parts.push(length, field);
  }
  return Buffer.concat(parts);
}

function patternedBuffer(size: number, seed: number) {
  const buffer = Buffer.allocUnsafe(size);
  for (let index = 0; index < size; index++) {
    buffer[index] = (index * 31 + seed) & 0xff;
  }
  return buffer;
}
