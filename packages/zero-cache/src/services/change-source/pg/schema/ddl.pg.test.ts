import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../../shared/src/logging-test-utils.ts';
import {testDBs} from '../../../../test/db.ts';
import type {PostgresDB} from '../../../../types/pg.ts';
import {setupTablesAndReplication} from './shard.ts';

const APP_ID = 'ddltest';
const SHARD_NUM = 0;
const SLOT = 'ddltest_messages';

type DdlMessage = {
  transactional: boolean;
  type: string;
  tag: string;
  previousIndexes: string[] | undefined;
  indexes: string[] | undefined;
};

// Reads the shard's ddl messages off a test_decoding slot. test_decoding
// renders a logical message as:
//   message: transactional: 1 prefix: <prefix>, sz: <n> content:<content>
const MESSAGE =
  /^message: transactional: (\d) prefix: ([^,]*), sz: \d+ content:(.*)$/s;

type IndexSpec = {schema: string; name: string};
type SchemaSpec = {indexes: IndexSpec[]} | null;

// The app's own (published) indexes, leaving out the shard's metadata tables.
function indexNames(schema: SchemaSpec | undefined) {
  return schema?.indexes
    .filter(({schema}) => schema === 'public')
    .map(({schema, name}) => `${schema}.${name}`)
    .sort();
}

async function ddlMessages(db: PostgresDB): Promise<DdlMessage[]> {
  const rows = await db<{data: string}[]>`
    SELECT data FROM pg_logical_slot_get_changes(${SLOT}, NULL, NULL)`;
  const messages: DdlMessage[] = [];
  for (const {data} of rows) {
    const match = MESSAGE.exec(data);
    if (!match || match[2] !== `${APP_ID}/${SHARD_NUM}/ddl`) {
      continue;
    }
    const content = JSON.parse(match[3]) as {
      type: string;
      event: {tag: string};
      previousSchema?: SchemaSpec;
      schema?: SchemaSpec;
    };
    messages.push({
      transactional: match[1] === '1',
      type: content.type,
      tag: content.event.tag,
      previousIndexes: indexNames(content.previousSchema),
      indexes: indexNames(content.schema),
    });
  }
  return messages;
}

async function indexExists(db: PostgresDB, schema: string, name: string) {
  const [{exists}] = await db<{exists: boolean}[]>`
    SELECT EXISTS (
      SELECT 1 FROM pg_indexes WHERE schemaname = ${schema} AND indexname = ${name}
    ) AS "exists"`;
  return exists;
}

describe('ddl event triggers: DROP INDEX', () => {
  let db: PostgresDB;

  beforeEach(async () => {
    db = await testDBs.create('ddl_drop_index_test');
    await db.unsafe(`
      CREATE TABLE public.foo (id TEXT PRIMARY KEY, val TEXT);
      CREATE INDEX foo_val ON public.foo (val);

      -- Outside the (default) publication, which covers schema "public".
      CREATE SCHEMA other;
      CREATE TABLE other.bar (id TEXT PRIMARY KEY, val TEXT);
      CREATE INDEX bar_val ON other.bar (val);
    `);
    await db.begin(tx =>
      setupTablesAndReplication(createSilentLogContext(), tx, {
        appID: APP_ID,
        shardNum: SHARD_NUM,
        publications: [],
      }),
    );
    await db`SELECT pg_create_logical_replication_slot(${SLOT}, 'test_decoding')`;
  });

  afterEach(async () => {
    await db`SELECT pg_drop_replication_slot(${SLOT})`;
    await testDBs.drop(db);
  });

  test('DROP INDEX CONCURRENTLY on a table outside the publications', async () => {
    await db.unsafe(`DROP INDEX CONCURRENTLY other.bar_val`);

    expect(await indexExists(db, 'other', 'bar_val')).toBe(false);
    // Nothing about the published schema changed, so nothing is emitted.
    expect((await ddlMessages(db)).filter(m => m.type !== 'ddlStart')).toEqual(
      [],
    );
  });

  test('DROP INDEX CONCURRENTLY IF EXISTS on a missing index', async () => {
    await db.unsafe(`DROP INDEX CONCURRENTLY IF EXISTS other.no_such_index`);
    expect((await ddlMessages(db)).filter(m => m.type !== 'ddlStart')).toEqual(
      [],
    );
  });

  test('DROP INDEX CONCURRENTLY on a published table is replicated', async () => {
    await db.unsafe(`DROP INDEX CONCURRENTLY public.foo_val`);

    expect(await indexExists(db, 'public', 'foo_val')).toBe(false);
    const updates = (await ddlMessages(db)).filter(m => m.type !== 'ddlStart');
    expect(updates).toEqual([
      {
        transactional: true,
        type: 'ddlUpdate',
        tag: 'DROP INDEX',
        previousIndexes: ['public.foo_pkey', 'public.foo_val'],
        indexes: ['public.foo_pkey'],
      },
    ]);
  });

  test('DROP INDEX on a published table is replicated', async () => {
    await db.unsafe(`DROP INDEX public.foo_val`);

    const updates = (await ddlMessages(db)).filter(m => m.type !== 'ddlStart');
    expect(updates).toEqual([
      {
        transactional: true,
        type: 'ddlUpdate',
        tag: 'DROP INDEX',
        previousIndexes: ['public.foo_pkey', 'public.foo_val'],
        indexes: ['public.foo_pkey'],
      },
    ]);
  });

  test('DROP INDEX in a transaction with other DDL is replicated', async () => {
    await db.begin(async tx => {
      await tx.unsafe(`CREATE TABLE other.baz (id TEXT PRIMARY KEY)`);
      await tx.unsafe(`DROP INDEX public.foo_val`);
      await tx.unsafe(`ALTER TABLE public.foo ADD COLUMN extra TEXT`);
    });

    const updates = (await ddlMessages(db)).filter(m => m.type !== 'ddlStart');
    expect(updates).toEqual([
      {
        transactional: true,
        type: 'ddlUpdate',
        tag: 'DROP INDEX',
        previousIndexes: ['public.foo_pkey', 'public.foo_val'],
        indexes: ['public.foo_pkey'],
      },
      {
        transactional: true,
        type: 'ddlUpdate',
        tag: 'ALTER TABLE',
        previousIndexes: ['public.foo_pkey'],
        indexes: ['public.foo_pkey'],
      },
    ]);
  });

  test('CREATE INDEX CONCURRENTLY is unaffected', async () => {
    await db.unsafe(`CREATE INDEX CONCURRENTLY foo_val2 ON public.foo (val)`);

    const updates = (await ddlMessages(db)).filter(m => m.type !== 'ddlStart');
    expect(updates).toEqual([
      {
        transactional: true,
        type: 'ddlUpdate',
        tag: 'CREATE INDEX',
        previousIndexes: ['public.foo_pkey', 'public.foo_val'],
        indexes: ['public.foo_pkey', 'public.foo_val', 'public.foo_val2'],
      },
    ]);
  });
});
