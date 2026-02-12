import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {
  CREATE_TABLE_METADATA_TABLE,
  TableMetadataTracker,
} from './table-metadata.ts';

describe('table-metadata', () => {
  let db: Database;
  let tracker: TableMetadataTracker;

  beforeEach(() => {
    db = new Database(createSilentLogContext(), ':memory:');
    db.exec(CREATE_TABLE_METADATA_TABLE);

    tracker = new TableMetadataTracker(db);
  });

  function dumpTable() {
    return db
      .prepare(
        /*sql*/ `SELECT * FROM "_zero.tableMetadata" ORDER BY "schema", "table"`,
      )
      .all();
  }

  test('set, rename, drop', () => {
    tracker.set({schema: 'public', name: 'foo'}, {rowKey: {columns: ['id']}});
    tracker.set(
      {schema: 'internal', name: 'bar'},
      {rowKey: {columns: ['a', 'b']}},
    );

    expect(dumpTable()).toMatchInlineSnapshot(`
      [
        {
          "metadata": "{"rowKey":{"columns":["a","b"]}}",
          "schema": "internal",
          "table": "bar",
        },
        {
          "metadata": "{"rowKey":{"columns":["id"]}}",
          "schema": "public",
          "table": "foo",
        },
      ]
    `);

    tracker.rename(
      {schema: 'internal', name: 'bar'},
      {schema: 'public', name: 'boo'},
    );
    expect(dumpTable()).toMatchInlineSnapshot(`
      [
        {
          "metadata": "{"rowKey":{"columns":["a","b"]}}",
          "schema": "public",
          "table": "boo",
        },
        {
          "metadata": "{"rowKey":{"columns":["id"]}}",
          "schema": "public",
          "table": "foo",
        },
      ]
    `);

    tracker.drop({schema: 'public', name: 'foo'});
    expect(dumpTable()).toMatchInlineSnapshot(`
      [
        {
          "metadata": "{"rowKey":{"columns":["a","b"]}}",
          "schema": "public",
          "table": "boo",
        },
      ]
    `);
  });
});
