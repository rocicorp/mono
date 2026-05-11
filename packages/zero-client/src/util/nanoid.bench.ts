import {randomUUID} from 'node:crypto';
import {bench, describe} from '../../../shared/src/bench.ts';
import {nanoid} from './nanoid.ts';

describe('ID generation', () => {
  bench('nanoid (Math.random, 21 chars)', () => nanoid());

  bench('crypto.randomUUID (36 chars)', () => randomUUID());

  bench('crypto.randomUUID stripped (32 chars)', () =>
    randomUUID().replaceAll('-', ''),
  );
});
