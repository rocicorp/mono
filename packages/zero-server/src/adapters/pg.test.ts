import {afterEach, expect, test, vi} from 'vitest';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';

const listeners = new Map<string, (e: Error) => void>();
vi.mock('pg', () => ({
  Pool: class {
    on(event: string, listener: (e: Error) => void) {
      listeners.set(event, listener);
      return this;
    }
  },
}));

const {zeroNodePg} = await import('./pg.ts');

const schema = createSchema({
  tables: [table('user').columns({id: string()}).primaryKey('id')],
});

afterEach(() => {
  listeners.clear();
  vi.restoreAllMocks();
});

test('a pool built from a connection string logs a pool error instead of leaving it uncaught', () => {
  const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
  zeroNodePg(schema, 'postgres://user:pass@localhost:5432/db');

  const onError = listeners.get('error');
  expect(onError).toBeDefined();

  const killed = Object.assign(
    new Error('terminating connection due to idle-in-transaction timeout'),
    {code: '25P03'},
  );
  // With no listener node-postgres would throw this; the listener must not.
  expect(() => onError!(killed)).not.toThrow();
  expect(warn).toHaveBeenCalledTimes(1);
  expect(warn.mock.calls[0].map(String).join(' ')).toContain(
    'node-postgres pool error',
  );
});
