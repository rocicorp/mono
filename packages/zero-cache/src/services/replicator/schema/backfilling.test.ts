import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {expectTables} from '../../../test/lite.ts';
import type {SchemaChange} from '../../change-source/protocol/current/data.ts';
import {
  BACKFILLING_TABLE,
  BackfillingTracker,
  CREATE_BACKFILLING_TABLE,
  populateBackfillingFromColumnMetadata,
  readBackfillDeclarations,
  readBackfillRequests,
} from './backfilling.ts';
import {CREATE_COLUMN_METADATA_TABLE} from './column-metadata.ts';
import {CREATE_TABLE_METADATA_TABLE} from './table-metadata.ts';

const lc = createSilentLogContext();

describe('replicator/schema/backfilling', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(lc, ':memory:');
    db.exec(
      CREATE_BACKFILLING_TABLE +
        CREATE_TABLE_METADATA_TABLE +
        CREATE_COLUMN_METADATA_TABLE,
    );
    return () => db.close();
  });

  /**
   * Every assertion below is about this one table's rows. The v18 resume
   * columns default to null so that the cookie-fold cases stay about the
   * fold; the cases that exercise them spell them out.
   */
  function expectBackfilling(rows: Record<string, unknown>[]) {
    expectTables(db, {
      [BACKFILLING_TABLE]: rows.map(row => ({
        mark: null,
        markWatermark: null,
        runID: null,
        minSnapshot: null,
        ...row,
      })),
    });
  }

  describe('readBackfillDeclarations', () => {
    function insert(
      table: string,
      column: string,
      resume: {
        mark?: string[] | null;
        markWatermark?: string | null;
        runID?: string | null;
      } = {},
    ) {
      db.prepare(
        `INSERT INTO "${BACKFILLING_TABLE}"
           ("schema", "table", "column", "backfill",
            "mark", "markWatermark", "runID")
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
      ).run(
        'public',
        table,
        column,
        '{"id":1}',
        resume.mark === undefined || resume.mark === null
          ? null
          : JSON.stringify(resume.mark),
        resume.markWatermark ?? null,
        resume.runID ?? null,
      );
    }

    test('nothing in flight declares nothing', () => {
      expect(readBackfillDeclarations(db)).toEqual([]);
    });

    test('one entry per table, with the columns in flight', () => {
      insert('issues', 'description', {
        mark: ['1'],
        markWatermark: '0a',
        runID: 'run-1',
      });
      insert('issues', 'assignee', {
        mark: ['1'],
        markWatermark: '0a',
        runID: 'run-1',
      });
      insert('comments', 'body');

      expect(readBackfillDeclarations(db)).toEqual([
        {
          schema: 'public',
          table: 'comments',
          columns: ['body'],
          metadata: null,
          backfill: {body: {id: 1}},
          mark: null,
          markWatermark: null,
          runID: null,
        },
        {
          schema: 'public',
          table: 'issues',
          columns: ['assignee', 'description'],
          metadata: null,
          backfill: {assignee: {id: 1}, description: {id: 1}},
          mark: ['1'],
          markWatermark: '0a',
          runID: 'run-1',
        },
      ]);
    });

    test('columns that disagree null out the field they disagree on', () => {
      // A column added to a table whose backfill was already under way.
      insert('issues', 'description', {
        mark: ['1'],
        markWatermark: '0a',
        runID: 'run-1',
      });
      insert('issues', 'assignee', {runID: 'run-1'});

      expect(readBackfillDeclarations(db)).toEqual([
        {
          schema: 'public',
          table: 'issues',
          columns: ['assignee', 'description'],
          metadata: null,
          backfill: {assignee: {id: 1}, description: {id: 1}},
          // Declaring a mark that only some columns have reached would
          // complete the others early, so the table restarts instead.
          mark: null,
          markWatermark: null,
          // They do agree on the run, so the run is still declared.
          runID: 'run-1',
        },
      ]);
    });

    test('columns that disagree on the run null out only the run', () => {
      insert('issues', 'description', {
        mark: ['1'],
        markWatermark: '0a',
        runID: 'run-1',
      });
      insert('issues', 'assignee', {mark: ['1'], markWatermark: '0a'});

      expect(readBackfillDeclarations(db)).toEqual([
        {
          schema: 'public',
          table: 'issues',
          columns: ['assignee', 'description'],
          metadata: null,
          backfill: {assignee: {id: 1}, description: {id: 1}},
          mark: ['1'],
          markWatermark: '0a',
          runID: null,
        },
      ]);
    });
  });

  describe('BackfillingTracker', () => {
    function apply(...changes: SchemaChange[]) {
      const tracker = new BackfillingTracker(db);
      changes.forEach(change => tracker.apply(change));
    }

    test('create-table records one row per backfilling column', () => {
      apply({
        tag: 'create-table',
        spec: {schema: 'my', name: 'foo', columns: {}},
        metadata: {rowKey: {type: 'default', columns: ['id']}},
        backfill: {a: {fooID: 987}, b: {fooID: 843}},
      });

      expectBackfilling([
        {schema: 'my', table: 'foo', column: 'a', backfill: '{"fooID":987}'},
        {schema: 'my', table: 'foo', column: 'b', backfill: '{"fooID":843}'},
      ]);
    });

    test('changes carrying no backfill record nothing', () => {
      apply(
        // A change source that does not support backfill sends neither field.
        {
          tag: 'create-table',
          spec: {schema: 'public', name: 'foo', columns: {}},
        },
        {
          tag: 'add-column',
          table: {schema: 'public', name: 'foo'},
          column: {name: 'a', spec: {pos: 1, dataType: 'text'}},
        },
        // Metadata-only changes move the metadata cookie, which lives in
        // "_zero.tableMetadata".
        {
          tag: 'update-table-metadata',
          table: {schema: 'public', name: 'foo'},
          old: {rowKey: {type: 'default', columns: ['id']}},
          new: {rowKey: {type: 'index', columns: ['a']}},
        },
        {
          tag: 'create-index',
          spec: {
            schema: 'public',
            name: 'foo_idx',
            tableName: 'foo',
            unique: false,
            columns: {a: 'ASC'},
          },
        },
        {tag: 'drop-index', id: {schema: 'public', name: 'foo_idx'}},
      );

      expectBackfilling([]);
    });

    test('add-column upserts, update-column renames only on a rename', () => {
      apply(
        {
          tag: 'add-column',
          table: {schema: 'my', name: 'foo'},
          column: {name: 'a', spec: {pos: 1, dataType: 'text'}},
          backfill: {fooID: 1},
        },
        // A spec-only update must move nothing.
        {
          tag: 'update-column',
          table: {schema: 'my', name: 'foo'},
          old: {name: 'a', spec: {pos: 1, dataType: 'text'}},
          new: {name: 'a', spec: {pos: 1, dataType: 'int4'}},
        },
      );
      expectBackfilling([
        {schema: 'my', table: 'foo', column: 'a', backfill: '{"fooID":1}'},
      ]);

      apply({
        tag: 'update-column',
        table: {schema: 'my', name: 'foo'},
        old: {name: 'a', spec: {pos: 1, dataType: 'int4'}},
        new: {name: 'z', spec: {pos: 1, dataType: 'int4'}},
      });
      expectBackfilling([
        {schema: 'my', table: 'foo', column: 'z', backfill: '{"fooID":1}'},
      ]);
    });

    test('rename-table moves only the renamed table, drop-table clears it', () => {
      apply(
        {
          tag: 'create-table',
          spec: {schema: 'my', name: 'foo', columns: {}},
          backfill: {a: {fooID: 1}},
        },
        {
          tag: 'create-table',
          spec: {schema: 'your', name: 'bar', columns: {}},
          backfill: {c: {fooID: 2}},
        },
        {
          tag: 'rename-table',
          old: {schema: 'my', name: 'foo'},
          new: {schema: 'renamed', name: 'foo'},
        },
      );
      expectBackfilling([
        {
          schema: 'renamed',
          table: 'foo',
          column: 'a',
          backfill: '{"fooID":1}',
        },
        {schema: 'your', table: 'bar', column: 'c', backfill: '{"fooID":2}'},
      ]);

      apply({tag: 'drop-table', id: {schema: 'renamed', name: 'foo'}});
      expectBackfilling([
        {schema: 'your', table: 'bar', column: 'c', backfill: '{"fooID":2}'},
      ]);
    });

    test('drop-column and backfill-completed clear columns', () => {
      apply({
        tag: 'create-table',
        spec: {schema: 'my', name: 'foo', columns: {}},
        backfill: {id: {fooID: 0}, a: {fooID: 1}, b: {fooID: 2}},
      });

      apply({
        tag: 'drop-column',
        table: {schema: 'my', name: 'foo'},
        column: 'b',
      });
      expectBackfilling([
        {schema: 'my', table: 'foo', column: 'a', backfill: '{"fooID":1}'},
        {schema: 'my', table: 'foo', column: 'id', backfill: '{"fooID":0}'},
      ]);

      // The rowKey columns are excluded from `columns` but are backfilled with
      // them, so both are cleared.
      apply({
        tag: 'backfill-completed',
        relation: {
          schema: 'my',
          name: 'foo',
          rowKey: {type: 'default', columns: ['id']},
        },
        columns: ['a'],
        watermark: '07',
      });
      expectBackfilling([]);
    });
  });

  describe('readBackfillRequests', () => {
    test('empty', () => {
      expect(readBackfillRequests(db)).toEqual([]);
    });

    test('groups by table, with metadata from "_zero.tableMetadata"', () => {
      const tracker = new BackfillingTracker(db);
      tracker.apply({
        tag: 'create-table',
        spec: {schema: 'my', name: 'foo', columns: {}},
        backfill: {b: {fooID: 2}, a: {fooID: 1}},
      });
      tracker.apply({
        tag: 'create-table',
        spec: {schema: 'public', name: 'bar', columns: {}},
        backfill: {c: {barID: 'three'}},
      });
      db.prepare(/*sql*/ `INSERT INTO "_zero.tableMetadata"
                   ("schema", "table", "upstreamMetadata") VALUES (?, ?, ?)`).run(
        'my',
        'foo',
        '{"rowKey":{"type":"default","columns":["id"]}}',
      );

      expect(readBackfillRequests(db)).toEqual([
        {
          table: {
            schema: 'my',
            name: 'foo',
            metadata: {rowKey: {type: 'default', columns: ['id']}},
          },
          columns: {a: {fooID: 1}, b: {fooID: 2}},
        },
        // A table can be backfilling with no metadata of its own.
        {
          table: {schema: 'public', name: 'bar', metadata: null},
          columns: {c: {barID: 'three'}},
        },
      ]);
    });

    test('a "_zero.tableMetadata" row without metadata reads as null', () => {
      new BackfillingTracker(db).apply({
        tag: 'add-column',
        table: {schema: 'public', name: 'foo'},
        column: {name: 'a', spec: {pos: 1, dataType: 'text'}},
        backfill: {fooID: 1},
      });
      // "_zero.tableMetadata" also tracks minRowVersion, so a row can exist
      // with a null upstreamMetadata.
      db.prepare(/*sql*/ `INSERT INTO "_zero.tableMetadata"
                   ("schema", "table", "minRowVersion") VALUES (?, ?, ?)`).run(
        'public',
        'foo',
        '03',
      );

      expect(readBackfillRequests(db)).toEqual([
        {
          table: {schema: 'public', name: 'foo', metadata: null},
          columns: {a: {fooID: 1}},
        },
      ]);
    });
  });

  describe('populateBackfillingFromColumnMetadata', () => {
    function addColumnMetadata(
      liteTable: string,
      column: string,
      backfill: string | null,
    ) {
      db.prepare(/*sql*/ `INSERT INTO "_zero.column_metadata"
          (table_name, column_name, upstream_type, is_not_null, is_enum,
           is_array, backfill)
          VALUES (?, ?, 'text', 0, 0, 0, ?)`).run(liteTable, column, backfill);
    }

    function addTableMetadata(schema: string, table: string) {
      db.prepare(/*sql*/ `INSERT INTO "_zero.tableMetadata"
                   ("schema", "table", "upstreamMetadata") VALUES (?, ?, ?)`).run(
        schema,
        table,
        '{"rowKey":{"type":"default","columns":["id"]}}',
      );
    }

    test('nothing to copy, which is the expected case', () => {
      addColumnMetadata('foo', 'a', null);

      populateBackfillingFromColumnMetadata(lc, db);

      expectBackfilling([]);
    });

    test('copies only in-flight backfills', () => {
      addColumnMetadata('foo', 'a', '{"fooID":1}');
      addColumnMetadata('foo', 'b', null);

      populateBackfillingFromColumnMetadata(lc, db);

      expectBackfilling([
        {
          schema: 'public',
          table: 'foo',
          column: 'a',
          backfill: '{"fooID":1}',
        },
      ]);
    });

    test('resolves the schema through "_zero.tableMetadata"', () => {
      addTableMetadata('my', 'foo');
      addColumnMetadata('my.foo', 'a', '{"fooID":1}');

      populateBackfillingFromColumnMetadata(lc, db);

      expectBackfilling([
        {schema: 'my', table: 'foo', column: 'a', backfill: '{"fooID":1}'},
      ]);
    });

    test('a dotted table name is resolved even when the dot is in the name', () => {
      // `liteTableName({schema: 'public', name: 'a.b'})` and
      // `liteTableName({schema: 'a', name: 'b'})` are the same string, so the
      // metadata row is the only thing that tells them apart.
      addTableMetadata('public', 'a.b');
      addColumnMetadata('a.b', 'c', '{"fooID":1}');

      populateBackfillingFromColumnMetadata(lc, db);

      expectBackfilling([
        {
          schema: 'public',
          table: 'a.b',
          column: 'c',
          backfill: '{"fooID":1}',
        },
      ]);
    });

    test('an unresolvable row falls back to the first-dot split', () => {
      addColumnMetadata('my.foo', 'a', '{"fooID":1}');
      // Not unresolvable: liteTableName() only omits the schema for `public`,
      // so a name with no dot in it is exactly `public`.
      addColumnMetadata('bar', 'b', '{"fooID":2}');

      populateBackfillingFromColumnMetadata(lc, db);

      expectBackfilling([
        {
          schema: 'public',
          table: 'bar',
          column: 'b',
          backfill: '{"fooID":2}',
        },
        {schema: 'my', table: 'foo', column: 'a', backfill: '{"fooID":1}'},
      ]);
    });

    test('replaces rather than merges, so a re-run after a rollback is correct', () => {
      addTableMetadata('my', 'foo');
      addColumnMetadata('my.foo', 'a', '{"fooID":1}');
      populateBackfillingFromColumnMetadata(lc, db);

      // What an older zero-cache, which does not know about this table, does
      // to the replica while it is rolled back: the backfill completes and is
      // cleared from column_metadata, and a new one starts.
      db.prepare(/*sql*/ `UPDATE "_zero.column_metadata" SET backfill = NULL
                   WHERE table_name = ? AND column_name = ?`).run(
        'my.foo',
        'a',
      );
      addColumnMetadata('my.foo', 'z', '{"fooID":9}');

      populateBackfillingFromColumnMetadata(lc, db);

      expectBackfilling([
        {schema: 'my', table: 'foo', column: 'z', backfill: '{"fooID":9}'},
      ]);
    });
  });
});
