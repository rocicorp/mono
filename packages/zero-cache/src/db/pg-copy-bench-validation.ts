import {createHash} from 'node:crypto';
import {BinaryCopyParser} from './pg-copy-binary.ts';

export type BinaryCopyContentSummary = {
  fields: number;
  payloadBytes: number;
  digest: string;
};

export function summarizeBinaryCopyFields(
  fields: Iterable<Buffer | null>,
): BinaryCopyContentSummary {
  const hash = createHash('sha256');
  let fieldCount = 0;
  let payloadBytes = 0;
  for (const field of fields) {
    fieldCount++;
    const length = Buffer.alloc(4);
    length.writeInt32BE(field?.length ?? -1);
    hash.update(length);
    if (field !== null) {
      payloadBytes += field.length;
      hash.update(field);
    }
  }
  return {fields: fieldCount, payloadBytes, digest: hash.digest('hex')};
}

export function validateBinaryCopyContent(
  chunks: readonly Buffer[],
  expected: BinaryCopyContentSummary,
) {
  const parser = new BinaryCopyParser();
  function* parsedFields() {
    for (const chunk of chunks) {
      yield* parser.parse(chunk);
    }
  }
  const actual = summarizeBinaryCopyFields(parsedFields());
  if (
    actual.fields !== expected.fields ||
    actual.payloadBytes !== expected.payloadBytes ||
    actual.digest !== expected.digest
  ) {
    throw new Error(
      `binary COPY content mismatch: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`,
    );
  }
}
