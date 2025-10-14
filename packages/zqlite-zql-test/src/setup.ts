import {afterAll, beforeAll} from 'vitest';
import {createSource} from '../../zqlite/src/test/source-factory.ts';

beforeAll(() => {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  (globalThis as any).sourceFactory = createSource;
});
afterAll(() => {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  delete (globalThis as any).sourceFactory;
});
