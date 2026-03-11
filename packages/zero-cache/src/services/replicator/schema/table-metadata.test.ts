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
    expect(tracker.getMinRowVersions()).toMatchInlineSnapshot(`Map {}`);

    tracker.setUpstreamMetadata(
      {schema: 'public', name: 'foo'},
      {rowKey: {columns: ['id']}},
    );
    tracker.setMinRowVersion({schema: 'internal', name: 'bar'}, '123');

    // Rows are inserted with defaults for the other columns.
    expect(dumpTable()).toMatchInlineSnapshot(`
      [
        {
          "minRowVersion": "123",
          "schema": "internal",
          "table": "bar",
          "upstreamMetadata": null,
        },
        {
          "minRowVersion": "00",
          "schema": "public",
          "table": "foo",
          "upstreamMetadata": "{"rowKey":{"columns":["id"]}}",
        },
      ]
    `);

    expect(tracker.getMinRowVersions()).toMatchInlineSnapshot(`
      Map {
        "foo" => "00",
        "internal.bar" => "123",
      }
    `);

    tracker.setMinRowVersion({schema: 'public', name: 'foo'}, '2b8a');
    tracker.setUpstreamMetadata(
      {schema: 'internal', name: 'bar'},
      {rowKey: {columns: ['a', 'b']}},
    );

    // Rows are updated, preserving the other columns.
    expect(dumpTable()).toMatchInlineSnapshot(`
      [
        {
          "minRowVersion": "123",
          "schema": "internal",
          "table": "bar",
          "upstreamMetadata": "{"rowKey":{"columns":["a","b"]}}",
        },
        {
          "minRowVersion": "2b8a",
          "schema": "public",
          "table": "foo",
          "upstreamMetadata": "{"rowKey":{"columns":["id"]}}",
        },
      ]
    `);

    expect(tracker.getMinRowVersions()).toMatchInlineSnapshot(`
      Map {
        "foo" => "2b8a",
        "internal.bar" => "123",
      }
    `);

    tracker.rename(
      {schema: 'internal', name: 'bar'},
      {schema: 'public', name: 'boo'},
    );
    expect(dumpTable()).toMatchInlineSnapshot(`
      [
        {
          "minRowVersion": "123",
          "schema": "public",
          "table": "boo",
          "upstreamMetadata": "{"rowKey":{"columns":["a","b"]}}",
        },
        {
          "minRowVersion": "2b8a",
          "schema": "public",
          "table": "foo",
          "upstreamMetadata": "{"rowKey":{"columns":["id"]}}",
        },
      ]
    `);

    expect(tracker.getMinRowVersions()).toMatchInlineSnapshot(`
      Map {
        "foo" => "2b8a",
        "boo" => "123",
      }
    `);

    tracker.drop({schema: 'public', name: 'foo'});
    expect(dumpTable()).toMatchInlineSnapshot(`
      [
        {
          "minRowVersion": "123",
          "schema": "public",
          "table": "boo",
          "upstreamMetadata": "{"rowKey":{"columns":["a","b"]}}",
        },
      ]
    `);

    expect(tracker.getMinRowVersions()).toMatchInlineSnapshot(`
      Map {
        "boo" => "123",
      }
    `);
  });
});
