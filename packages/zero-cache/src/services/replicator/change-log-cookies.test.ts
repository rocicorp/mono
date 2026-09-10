import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import type {
  MessageUpdate,
  SchemaChange,
} from '../change-source/protocol/current/data.ts';
import {
  backfillRequestsFrom,
  ChangeLogCookieWriter,
  CREATE_CHANGE_LOG_COOKIE_SCHEMA,
  cookieOps,
  EMPTY_COOKIE_SET,
  foldCookies,
  markOps,
  readCookieRowCounts,
  readCookies,
  replaceCookies,
  type CookieOp,
  type CookieSet,
} from './change-log-cookies.ts';

const lc = createSilentLogContext();

function createCookieJar(): Database {
  const db = new Database(lc, ':memory:');
  db.exec(CREATE_CHANGE_LOG_COOKIE_SCHEMA);
  return db;
}

/**
 * The cookie set after folding `changes`, without `minSnapshot`.
 *
 * The schema-change fold never moves `minSnapshot` -- it is stamped at write
 * time from `update` messages, with the interpreter's own transaction version
 * (see `BackfillCookie.minSnapshot`) -- so the cases below stay about what the
 * fold does. `readCookies` is used directly where `minSnapshot` is the point.
 */
type FoldedCookies = {
  tableMetadata: CookieSet['tableMetadata'];
  backfilling: Omit<CookieSet['backfilling'][number], 'minSnapshot'>[];
};

function fold(db: Database, ...changes: SchemaChange[]): FoldedCookies {
  const cookies = foldInto(db, ...changes);
  return {
    tableMetadata: cookies.tableMetadata,
    backfilling: cookies.backfilling.map(
      ({minSnapshot: _, ...cookie}) => cookie,
    ),
  };
}

/** Folds a change sequence through the SQLite interpreter. */
function foldInto(db: Database, ...changes: SchemaChange[]): CookieSet {
  const writer = new ChangeLogCookieWriter(db);
  changes.forEach(change => writer.apply(change));
  return readCookies(db);
}

const ROW_KEY: {columns: string[]; type: 'default'} = {
  columns: ['id'],
  type: 'default',
};

const CREATE_FOO: SchemaChange = {
  tag: 'create-table',
  spec: {schema: 'my', name: 'foo', columns: {}},
  metadata: {rowKey: {type: 'index', columns: ['a', 'b']}},
  backfill: {
    a: {fooID: 987, barID: 'zoo'},
    b: {fooID: 843, barID: 'ozz'},
  },
};

describe('replicator/change-log-cookies', () => {
  describe('cookieOps', () => {
    // The fold is what keeps the Postgres and SQLite change logs from drifting,
    // so each transition's ops are pinned rather than round-tripped.
    const cases: [name: string, change: SchemaChange, ops: CookieOp[]][] = [
      [
        'create-table with metadata and backfills',
        CREATE_FOO,
        [
          {
            op: 'upsert-metadata',
            table: {schema: 'my', name: 'foo'},
            metadata: {rowKey: {type: 'index', columns: ['a', 'b']}},
          },
          {
            op: 'upsert-backfill',
            table: {schema: 'my', name: 'foo'},
            column: 'a',
            backfill: {fooID: 987, barID: 'zoo'},
          },
          {
            op: 'upsert-backfill',
            table: {schema: 'my', name: 'foo'},
            column: 'b',
            backfill: {fooID: 843, barID: 'ozz'},
          },
        ],
      ],
      [
        // Both fields are optional in the protocol, and a change source that
        // does not support backfill sends neither.
        'create-table without metadata or backfills',
        {
          tag: 'create-table',
          spec: {schema: 'my', name: 'foo', columns: {}},
        },
        [],
      ],
      [
        'update-table-metadata',
        {
          tag: 'update-table-metadata',
          table: {schema: 'my', name: 'foo'},
          old: {rowKey: {type: 'index', columns: ['a']}},
          new: {rowKey: {type: 'default', columns: ['b']}},
        },
        [
          {
            op: 'upsert-metadata',
            table: {schema: 'my', name: 'foo'},
            metadata: {rowKey: {type: 'default', columns: ['b']}},
          },
        ],
      ],
      [
        'add-column with table metadata and a backfill',
        {
          tag: 'add-column',
          table: {schema: 'my', name: 'foo'},
          tableMetadata: {rowKey: {type: 'default', columns: ['b']}},
          column: {name: 'c', spec: {pos: 3, dataType: 'text'}},
          backfill: {fooID: 123, barID: 'baz'},
        },
        [
          {
            op: 'upsert-metadata',
            table: {schema: 'my', name: 'foo'},
            metadata: {rowKey: {type: 'default', columns: ['b']}},
          },
          {
            op: 'upsert-backfill',
            table: {schema: 'my', name: 'foo'},
            column: 'c',
            backfill: {fooID: 123, barID: 'baz'},
          },
        ],
      ],
      [
        'add-column with neither',
        {
          tag: 'add-column',
          table: {schema: 'my', name: 'foo'},
          column: {name: 'c', spec: {pos: 3, dataType: 'text'}},
        },
        [],
      ],
      [
        'rename-table',
        {
          tag: 'rename-table',
          old: {schema: 'my', name: 'foo'},
          new: {schema: 'your', name: 'bar'},
        },
        [
          {
            op: 'rename-table',
            old: {schema: 'my', name: 'foo'},
            new: {schema: 'your', name: 'bar'},
          },
        ],
      ],
      [
        'drop-table',
        {tag: 'drop-table', id: {schema: 'my', name: 'foo'}},
        [{op: 'drop-table', table: {schema: 'my', name: 'foo'}}],
      ],
      [
        'update-column that renames',
        {
          tag: 'update-column',
          table: {schema: 'my', name: 'foo'},
          old: {name: 'a', spec: {pos: 1, dataType: 'text'}},
          new: {name: 'z', spec: {pos: 1, dataType: 'text'}},
        },
        [
          {
            op: 'rename-column',
            table: {schema: 'my', name: 'foo'},
            old: 'a',
            new: 'z',
          },
        ],
      ],
      [
        // A type change moves nothing: whether a column is backfilling is not a
        // property of its spec.
        'update-column that does not rename',
        {
          tag: 'update-column',
          table: {schema: 'my', name: 'foo'},
          old: {name: 'a', spec: {pos: 1, dataType: 'text'}},
          new: {name: 'a', spec: {pos: 1, dataType: 'int4'}},
        },
        [],
      ],
      [
        'drop-column',
        {tag: 'drop-column', table: {schema: 'my', name: 'foo'}, column: 'a'},
        [
          {
            op: 'drop-column',
            table: {schema: 'my', name: 'foo'},
            column: 'a',
          },
        ],
      ],
      [
        // The rowKey columns are excluded from `columns` but are backfilled
        // with them, so the op clears both.
        'backfill-completed',
        {
          tag: 'backfill-completed',
          relation: {schema: 'my', name: 'foo', rowKey: ROW_KEY},
          columns: ['a', 'b'],
          watermark: '07',
        },
        [
          {
            op: 'complete-backfill',
            table: {schema: 'my', name: 'foo'},
            columns: ['id', 'a', 'b'],
          },
        ],
      ],
      [
        'create-index',
        {
          tag: 'create-index',
          spec: {
            schema: 'my',
            name: 'foo_idx',
            tableName: 'foo',
            unique: false,
            columns: {a: 'ASC'},
          },
        },
        [],
      ],
      [
        'drop-index',
        {tag: 'drop-index', id: {schema: 'my', name: 'foo_idx'}},
        [],
      ],
    ];

    test.each(cases)('%s', (_name, change, ops) => {
      expect(cookieOps(change)).toEqual(ops);
    });

    test('covers every schema-change tag', () => {
      // The fold is exhaustive at compile time; this is the reminder that a new
      // tag also needs a case above, including if its answer is "no ops".
      expect(new Set(cases.map(([, change]) => change.tag)).size).toBe(10);
    });
  });

  describe('markOps', () => {
    const relation = {
      schema: 'my',
      name: 'foo',
      rowKey: {columns: ['a', 'b']},
    };

    test('an update with no key is not a key change', () => {
      expect(
        markOps({tag: 'update', relation, key: null, new: {a: 1, b: 2}}),
      ).toEqual([]);
    });

    test('an update whose key columns are unchanged is not a key change', () => {
      // Replica identity FULL sends every column as the key, so most of these
      // carry a key without having moved one.
      expect(
        markOps({
          tag: 'update',
          relation,
          key: {a: 1, b: 2, c: 'old'},
          new: {a: 1, b: 2, c: 'new'},
        }),
      ).toEqual([]);
    });

    test('a moved key column invalidates the table', () => {
      expect(
        markOps({
          tag: 'update',
          relation,
          key: {a: 1, b: 2},
          new: {a: 1, b: 3},
        }),
      ).toEqual([{op: 'invalidate-marks', table: {schema: 'my', name: 'foo'}}]);
    });
  });

  describe('minSnapshot', () => {
    const keyChange: MessageUpdate = {
      tag: 'update',
      relation: {schema: 'my', name: 'foo', rowKey: {columns: ['a']}},
      key: {a: 1},
      new: {a: 2},
    };

    test('a key change stamps every in-flight column of its table', () => {
      using db = createCookieJar();
      const writer = new ChangeLogCookieWriter(db);
      writer.apply(CREATE_FOO);
      expect(writer.applyUpdate(keyChange, '0a')).toEqual([
        {op: 'invalidate-marks', table: {schema: 'my', name: 'foo'}},
      ]);

      expect(
        readCookies(db).backfilling.map(({column, minSnapshot}) => ({
          column,
          minSnapshot,
        })),
      ).toEqual([
        {column: 'a', minSnapshot: '0a'},
        {column: 'b', minSnapshot: '0a'},
      ]);

      // A later key change moves it forward.
      writer.applyUpdate(keyChange, '0b');
      expect(
        readCookies(db).backfilling.map(({minSnapshot}) => minSnapshot),
      ).toEqual(['0b', '0b']);
    });

    test('an update that moves no key stamps nothing', () => {
      using db = createCookieJar();
      const writer = new ChangeLogCookieWriter(db);
      writer.apply(CREATE_FOO);
      expect(writer.applyUpdate({...keyChange, key: null}, '0a')).toEqual([]);
      expect(
        readCookies(db).backfilling.map(({minSnapshot}) => minSnapshot),
      ).toEqual([null, null]);
    });

    test('a key change on another table stamps nothing', () => {
      using db = createCookieJar();
      const writer = new ChangeLogCookieWriter(db);
      writer.apply(CREATE_FOO);
      writer.applyUpdate(
        {
          ...keyChange,
          relation: {schema: 'my', name: 'bar', rowKey: {columns: ['a']}},
        },
        '0a',
      );
      expect(
        readCookies(db).backfilling.map(({minSnapshot}) => minSnapshot),
      ).toEqual([null, null]);
    });

    test('a column backfilled after the key change starts clean', () => {
      using db = createCookieJar();
      const writer = new ChangeLogCookieWriter(db);
      writer.apply(CREATE_FOO);
      writer.applyUpdate(keyChange, '0a');
      writer.apply({
        tag: 'add-column',
        table: {schema: 'my', name: 'foo'},
        column: {name: 'c', spec: {pos: 3, dataType: 'text'}},
        backfill: {fooID: 5},
      });
      expect(
        readCookies(db).backfilling.map(({column, minSnapshot}) => ({
          column,
          minSnapshot,
        })),
      ).toEqual([
        {column: 'a', minSnapshot: '0a'},
        {column: 'b', minSnapshot: '0a'},
        // A backfill started after the key change is not bounded by it.
        {column: 'c', minSnapshot: null},
      ]);
    });

    test('an upsert of an existing column carries its minSnapshot forward', () => {
      using db = createCookieJar();
      const writer = new ChangeLogCookieWriter(db);
      writer.apply(CREATE_FOO);
      writer.applyUpdate(keyChange, '0a');
      // Re-announced (e.g. the change source re-sent the same add-column).
      writer.apply({
        tag: 'add-column',
        table: {schema: 'my', name: 'foo'},
        column: {name: 'a', spec: {pos: 1, dataType: 'text'}},
        backfill: {fooID: 987, barID: 'zoo'},
      });
      expect(
        readCookies(db).backfilling.map(({minSnapshot}) => minSnapshot),
      ).toEqual(['0a', '0a']);
    });

    test('the fold carries minSnapshot forward across an upsert', () => {
      const seeded: CookieSet = {
        tableMetadata: [],
        backfilling: [
          {
            schema: 'my',
            table: 'foo',
            column: 'a',
            backfill: {fooID: 1},
            minSnapshot: '0a',
          },
        ],
      };
      const folded = foldCookies(seeded, [
        {
          tag: 'add-column',
          table: {schema: 'my', name: 'foo'},
          column: {name: 'a', spec: {pos: 1, dataType: 'text'}},
          backfill: {fooID: 2},
        },
      ]);
      expect(folded.backfilling).toEqual([
        {
          schema: 'my',
          table: 'foo',
          column: 'a',
          backfill: {fooID: 2},
          minSnapshot: '0a',
        },
      ]);
    });

    test("backfillRequestsFrom carries the latest of a table's columns", () => {
      expect(
        backfillRequestsFrom({
          tableMetadata: [],
          backfilling: [
            {
              schema: 'my',
              table: 'foo',
              column: 'a',
              backfill: {fooID: 1},
              minSnapshot: '0a',
            },
            {
              schema: 'my',
              table: 'foo',
              column: 'b',
              backfill: {fooID: 2},
              minSnapshot: '0c',
            },
            {
              schema: 'my',
              table: 'bar',
              column: 'z',
              backfill: {fooID: 3},
              minSnapshot: null,
            },
          ],
        }),
      ).toEqual([
        {
          table: {schema: 'my', name: 'foo', metadata: null},
          columns: {a: {fooID: 1}, b: {fooID: 2}},
          minSnapshot: '0c',
        },
        {
          table: {schema: 'my', name: 'bar', metadata: null},
          columns: {z: {fooID: 3}},
        },
      ]);
    });
  });

  describe('the SQLite interpreter', () => {
    test('create-table seeds both cookies', () => {
      using db = createCookieJar();

      expect(fold(db, CREATE_FOO)).toEqual({
        tableMetadata: [
          {
            schema: 'my',
            table: 'foo',
            metadata: {rowKey: {type: 'index', columns: ['a', 'b']}},
          },
        ],
        backfilling: [
          {
            schema: 'my',
            table: 'foo',
            column: 'a',
            backfill: {fooID: 987, barID: 'zoo'},
          },
          {
            schema: 'my',
            table: 'foo',
            column: 'b',
            backfill: {fooID: 843, barID: 'ozz'},
          },
        ],
      });
    });

    test('metadata and backfills upsert rather than collide', () => {
      using db = createCookieJar();

      const {tableMetadata, backfilling} = fold(
        db,
        CREATE_FOO,
        {
          tag: 'update-table-metadata',
          table: {schema: 'my', name: 'foo'},
          old: {rowKey: {type: 'index', columns: ['a', 'b']}},
          new: {rowKey: {type: 'default', columns: ['id']}},
        },
        {
          tag: 'add-column',
          table: {schema: 'my', name: 'foo'},
          column: {name: 'a', spec: {pos: 1, dataType: 'text'}},
          backfill: {fooID: 111, barID: 'restarted'},
        },
      );

      expect(tableMetadata).toEqual([
        {
          schema: 'my',
          table: 'foo',
          metadata: {rowKey: {type: 'default', columns: ['id']}},
        },
      ]);
      expect(
        backfilling.map(({column, backfill}) => [column, backfill]),
      ).toEqual([
        ['a', {fooID: 111, barID: 'restarted'}],
        ['b', {fooID: 843, barID: 'ozz'}],
      ]);
    });

    test('a rename moves both cookies, and only the renamed table', () => {
      using db = createCookieJar();

      const cookies = fold(
        db,
        CREATE_FOO,
        {
          tag: 'create-table',
          spec: {schema: 'my', name: 'other', columns: {}},
          backfill: {a: {fooID: 1}},
        },
        {
          tag: 'rename-table',
          old: {schema: 'my', name: 'foo'},
          new: {schema: 'your', name: 'renamed'},
        },
      );

      expect(
        cookies.tableMetadata.map(({schema, table}) => [schema, table]),
      ).toEqual([['your', 'renamed']]);
      expect(
        cookies.backfilling.map(({schema, table, column}) => [
          schema,
          table,
          column,
        ]),
      ).toEqual([
        ['my', 'other', 'a'],
        ['your', 'renamed', 'a'],
        ['your', 'renamed', 'b'],
      ]);
    });

    test('a column rename moves only the named column', () => {
      using db = createCookieJar();

      const {backfilling} = fold(db, CREATE_FOO, {
        tag: 'update-column',
        table: {schema: 'my', name: 'foo'},
        old: {name: 'a', spec: {pos: 1, dataType: 'text'}},
        new: {name: 'z', spec: {pos: 1, dataType: 'text'}},
      });

      expect(
        backfilling.map(({column, backfill}) => [column, backfill]),
      ).toEqual([
        ['b', {fooID: 843, barID: 'ozz'}],
        ['z', {fooID: 987, barID: 'zoo'}],
      ]);
    });

    test('drop-column and backfill-completed clear backfills, not metadata', () => {
      using db = createCookieJar();

      const dropped = fold(db, CREATE_FOO, {
        tag: 'drop-column',
        table: {schema: 'my', name: 'foo'},
        column: 'a',
      });
      expect(dropped.backfilling.map(({column}) => column)).toEqual(['b']);
      expect(dropped.tableMetadata).toHaveLength(1);

      const completed = fold(db, {
        tag: 'backfill-completed',
        relation: {schema: 'my', name: 'foo', rowKey: ROW_KEY},
        columns: ['b'],
        watermark: '07',
      });
      // The table's metadata outlives its backfills: it is what the *next*
      // BackfillRequest for the table has to carry.
      expect(completed.backfilling).toEqual([]);
      expect(completed.tableMetadata).toHaveLength(1);
    });

    test('drop-table clears both cookies', () => {
      using db = createCookieJar();

      expect(
        fold(db, CREATE_FOO, {
          tag: 'drop-table',
          id: {schema: 'my', name: 'foo'},
        }),
      ).toEqual(EMPTY_COOKIE_SET);
    });

    test('a table with backfills and no metadata is legal', () => {
      using db = createCookieJar();

      const cookies = fold(db, {
        tag: 'create-table',
        spec: {schema: 'your', name: 'bar', columns: {}},
        backfill: {a: {fooID: 987}},
      });

      expect(cookies.tableMetadata).toEqual([]);
      expect(cookies.backfilling).toHaveLength(1);
    });

    test('apply reports the ops it ran', () => {
      using db = createCookieJar();
      const writer = new ChangeLogCookieWriter(db);

      expect(writer.apply(CREATE_FOO).map(({op}) => op)).toEqual([
        'upsert-metadata',
        'upsert-backfill',
        'upsert-backfill',
      ]);
      expect(
        writer.apply({
          tag: 'create-index',
          spec: {
            schema: 'my',
            name: 'foo_idx',
            tableName: 'foo',
            unique: false,
            columns: {a: 'ASC'},
          },
        }),
      ).toEqual([]);
    });
  });

  describe('readers', () => {
    test('replaceCookies replaces the whole set', () => {
      using db = createCookieJar();
      fold(db, CREATE_FOO);

      const replacement: CookieSet = {
        tableMetadata: [
          {
            schema: 'your',
            table: 'bar',
            metadata: {rowKey: {type: 'default', columns: ['id']}},
          },
        ],
        backfilling: [
          {
            schema: 'your',
            table: 'bar',
            column: 'c',
            backfill: {fooID: 5},
            minSnapshot: null,
          },
        ],
      };
      replaceCookies(db, replacement);
      expect(readCookies(db)).toEqual(replacement);

      replaceCookies(db, EMPTY_COOKIE_SET);
      expect(readCookies(db)).toEqual(EMPTY_COOKIE_SET);
    });

    test('round-trips values outside the safe integer range', () => {
      using db = createCookieJar();

      const cookies: CookieSet = {
        tableMetadata: [
          {
            schema: 'my',
            table: 'foo',
            metadata: {rowKey: {columns: ['id']}, oid: 9007199254740993n},
          },
        ],
        backfilling: [
          {
            schema: 'my',
            table: 'foo',
            column: 'a',
            backfill: {snapshotID: '000003E8-1', xmin: 1000},
            minSnapshot: null,
          },
        ],
      };
      replaceCookies(db, cookies);
      expect(readCookies(db)).toEqual(cookies);
    });

    test('row counts are per cookie table', () => {
      using db = createCookieJar();
      expect(readCookieRowCounts(db)).toEqual({
        tableMetadata: 0,
        backfilling: 0,
      });

      fold(db, CREATE_FOO);
      expect(readCookieRowCounts(db)).toEqual({
        tableMetadata: 1,
        backfilling: 2,
      });
    });
  });
});
