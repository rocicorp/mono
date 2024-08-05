import Database from 'better-sqlite3';
import {expect, test} from 'vitest';
import {newEntityQuery} from 'zql/src/zql/query/entity-query.js';
import {createContext} from './context.js';
import {ZQLite} from './ZQLite.js';

test('smoke test', async () => {
  const db = new Database(':memory:');
  const z = new ZQLite(db);
  const context = createContext(z, db);

  db.prepare('CREATE TABLE foo (id TEXT PRIMARY KEY, name TEXT)').run();

  type Foo = {
    id: string;
    name: string;
  };

  // The EntityQuery is what we would get from `zero.query`
  // in zero-client tests
  // e.g. const z = newZero();
  // const q = z.query.foo;
  const q = newEntityQuery<{foo: Foo}>(context, 'foo');

  // A source represents a table.
  // Adding to the source is like inserting into the table.
  // This is analogous to `mutators` in zero-client tests
  // e.g., z.mutate.foo.create({id: '1', name: 'one'});
  const source = context.getSource('foo');
  z.tx(() => {
    source.add({id: '1', name: 'one'});
    source.add({id: '2', name: 'two'});
    source.add({id: '3', name: 'three'});
  });

  const stmt = q.select('*').prepare();
  const rows = await stmt.exec();

  expect(rows).toEqual([
    {id: '1', name: 'one'},
    {id: '2', name: 'two'},
    {id: '3', name: 'three'},
  ]);

  stmt.destroy();
});
