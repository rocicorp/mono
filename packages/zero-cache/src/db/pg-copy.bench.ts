import {pipeline, Readable} from 'node:stream';
import {bench, describe} from 'vitest';
import {TextTransform} from './pg-copy.ts';

describe('pg-copy benchmark', () => {
  const row = Buffer.from(
    `abcde\\\\fghijkl\t12398393\t\\N\t3823.3828\t{"foo":"bar\\tbaz\\nbong"}\t\\N\t\tboo\n`,
  );
  bench('copy', () => {
    const readable = new Readable({read() {}});
    const transform = new TextTransform();
    pipeline(readable, transform, () => {});

    for (let i = 0; i < 1_000_000; i++) {
      readable.push(row);
    }
  });
});
