import {expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {relationships} from '../../../../zero-schema/src/builder/relationship-builder.ts';
import {
  clientSchemaFrom,
  createSchema,
} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import {buildPipeline} from '../../../../zql/src/builder/builder.ts';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import {MemorySource} from '../../../../zql/src/ivm/memory-source.ts';
import {MemoryStorage} from '../../../../zql/src/ivm/memory-storage.ts';
import {skipYields} from '../../../../zql/src/ivm/operator.ts';
import {makeSourceChangeAdd} from '../../../../zql/src/ivm/source.ts';
import {asQueryImpl, newQuery} from '../../../../zql/src/query/query-impl.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {PipelineDriver, type RowChange} from './pipeline-driver.ts';
import {Snapshotter} from './snapshotter.ts';
import {SHARD} from './view-syncer-test-util.ts';

const item = table('item')
  .columns({key: string(), boxKey: string()})
  .primaryKey('key');
const box = table('box')
  .columns({key: string(), shelfKey: string()})
  .primaryKey('key');
const shelf = table('shelf')
  .columns({key: string(), enabled: boolean()})
  .primaryKey('key');
const schema = createSchema({
  tables: [item, box, shelf],
  relationships: [
    relationships(item, ({one}) => ({
      box: one({
        sourceField: ['boxKey'],
        destField: ['key'],
        destSchema: box,
      }),
    })),
    relationships(box, ({one}) => ({
      shelf: one({
        sourceField: ['shelfKey'],
        destField: ['key'],
        destSchema: shelf,
      }),
    })),
  ],
});

const lc = createSilentLogContext();

function serverHydrate(ast: AST): RowChange[] {
  const replicaFile = new DbFile('scalar-nested-repro');
  const replica = replicaFile.connect(lc);
  initReplicationState(replica, ['zero_data'], '01');
  replica.pragma('journal_mode = WAL2');
  replica.exec(`
    CREATE TABLE item (key TEXT PRIMARY KEY, "boxKey" TEXT, _0_version TEXT NOT NULL);
    CREATE TABLE box (key TEXT PRIMARY KEY, "shelfKey" TEXT, _0_version TEXT NOT NULL);
    CREATE TABLE shelf (key TEXT PRIMARY KEY, enabled BOOL, _0_version TEXT NOT NULL);
    INSERT INTO item VALUES ('i', 'b', '01');
    INSERT INTO box VALUES ('b', 's', '01');
    INSERT INTO shelf VALUES ('s', 1, '01');
  `);
  const storageDB = new Database(lc, ':memory:');
  storageDB.prepare(CREATE_STORAGE_TABLE).run();
  const driver = new PipelineDriver(
    lc,
    testLogConfig,
    new Snapshotter(lc, replicaFile.path, SHARD),
    SHARD,
    new DatabaseStorage(storageDB).createClientGroupStorage('cg'),
    'cg',
    new InspectorDelegate(undefined),
    () => 200,
  );
  driver.init(clientSchemaFrom(schema).clientSchema);
  const timer = {elapsedLap: () => 0, totalElapsed: () => 0};
  const changes = [...driver.addQuery('hash', 'q1', ast, timer)].filter(
    (c): c is RowChange => c !== 'yield',
  );
  driver.destroy?.();
  return changes;
}

/** Runs `ast` the way the client does, over just the rows the server synced. */
function clientRun(ast: AST, synced: RowChange[]) {
  const sources: Record<string, MemorySource> = {};
  for (const [name, t] of Object.entries(schema.tables)) {
    sources[name] = new MemorySource(name, t.columns, t.primaryKey);
  }
  for (const c of synced) {
    if (c.type === ChangeType.ADD && c.row) {
      const row = {...c.row} as Record<string, unknown>;
      delete row._0_version;
      if (c.table === 'shelf') row.enabled = !!row.enabled;
      for (const _ of sources[c.table].push(
        makeSourceChangeAdd(row as never),
      )) {
        // drain
      }
    }
  }
  const input = buildPipeline(
    ast,
    {
      getSource: n => sources[n],
      createStorage: () => new MemoryStorage(),
      decorateInput: i => i,
      decorateSourceInput: i => i,
      decorateFilterInput: i => i,
      addEdge() {},
    },
    'client',
  );
  return Array.from(skipYields(input.fetch({})), n => n.row);
}

const q = () => newQuery(schema, 'item');

const cases = {
  'scalar, no nesting': q().whereExists('box', b => b.where('key', 'b'), {
    scalar: true,
  }),
  'nested, not scalar': q().whereExists('box', b =>
    b.where('key', 'b').whereExists('shelf', s => s.where('enabled', true)),
  ),
  'nested, scalar': q().whereExists(
    'box',
    b =>
      b.where('key', 'b').whereExists('shelf', s => s.where('enabled', true)),
    {scalar: true},
  ),
  'both scalar, inner unpinned': q().whereExists(
    'box',
    b =>
      b.where('key', 'b').whereExists('shelf', s => s.where('enabled', true), {
        // @ts-expect-error inner scalar needs a pinned unique key
        scalar: true,
      }),
    {scalar: true},
  ),
  'both scalar, inner pinned': q().whereExists(
    'box',
    b =>
      b
        .where('key', 'b')
        .whereExists('shelf', s => s.where('key', 's').where('enabled', true), {
          scalar: true,
        }),
    {scalar: true},
  ),
};

// The server resolves a scalar subquery to a literal and syncs only the
// subquery's top-level row as a companion, dropping the rows of any nested
// (non-scalar) EXISTS. The client evaluates the original AST and finds the
// nested relationship empty. Flip these to `test` once that is fixed.
const knownBroken = new Set(['nested, scalar', 'both scalar, inner unpinned']);

for (const [name, query] of Object.entries(cases)) {
  (knownBroken.has(name) ? test.fails : test)(name, () => {
    const ast = asQueryImpl(query).ast;
    const synced = serverHydrate(ast);
    const clientRows = clientRun(ast, synced);
    expect(clientRows.map(r => r.key)).toEqual(['i']);
  });
}
