import {afterAll, beforeAll} from 'vitest';
import {createSource} from '../../zqlite/src/test/source-factory.js';

beforeAll(() => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (global as any).sourceFactory = createSource;
});
afterAll(() => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  delete (global as any).sourceFactory;
});
