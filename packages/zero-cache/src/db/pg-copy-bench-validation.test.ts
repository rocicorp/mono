import {expect, test} from 'vitest';
import {
  summarizeBinaryCopyFields,
  validateBinaryCopyContent,
} from './pg-copy-bench-validation.ts';

test('detects corruption inside a binary COPY field', () => {
  const field = Buffer.alloc(64, 7);
  const expected = summarizeBinaryCopyFields([field]);
  const stream = copyStream(field);
  const corrupted = Buffer.from(stream);
  corrupted[19 + 2 + 4 + 32] ^= 0xff;

  expect(() => validateBinaryCopyContent([corrupted], expected)).toThrow(
    'binary COPY content mismatch',
  );
});

function copyStream(field: Buffer) {
  const header = Buffer.from([
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0, 0, 0,
    0, 0, 0, 0, 0,
  ]);
  const count = Buffer.alloc(2);
  count.writeInt16BE(1);
  const length = Buffer.alloc(4);
  length.writeInt32BE(field.length);
  const trailer = Buffer.alloc(2);
  trailer.writeInt16BE(-1);
  return Buffer.concat([header, count, length, field, trailer]);
}
