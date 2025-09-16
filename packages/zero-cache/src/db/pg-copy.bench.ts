/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
