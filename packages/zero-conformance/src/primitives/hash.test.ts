import {test} from 'vitest';
import {h128, h32, h64} from '../../../shared/src/hash.ts';
import {interestingStrings, Random, randomString} from '../random.ts';
import {emitVectors} from '../vectors.ts';

test('hash', () => {
  const r = new Random(0x4a54);
  const inputs = [...interestingStrings];
  // Cover every length class of the xxHash32 tail and stripe loops.
  for (let n = 0; n <= 40; n++) {
    inputs.push('x'.repeat(n));
  }
  inputs.push('a'.repeat(4096), 'b'.repeat(4097), '\u{1f600}'.repeat(1500));
  for (let i = 0; i < 100; i++) {
    inputs.push(randomString(r, 40));
  }
  emitVectors(
    'hash',
    'hash',
    'h32, h64, and h128 (shared/src/hash.ts) of strings; wide hashes in decimal.',
    inputs.map(s => ({
      input: s,
      h32: h32(s),
      h64: h64(s).toString(),
      h128: h128(s).toString(),
    })),
  );
});
