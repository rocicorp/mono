import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {
  CHANGE_LOG_STREAM_FOLD_TAG_INDEX,
  CHANGE_LOG_STREAM_TABLE,
  CREATE_CHANGE_LOG_STREAM_SCHEMA,
} from '../replicator/change-log-db.ts';
import {
  MAX_FOLD_SCAN_ROWS,
  READ_SCHEMA_CHANGES_SQL,
  readSchemaChanges,
} from './change-log-range.ts';

describe('change-streamer/change-log-range', () => {
  let db: Database;
  beforeEach(() => {
    db = new Database(createSilentLogContext(), ':memory:');
    db.exec(CREATE_CHANGE_LOG_STREAM_SCHEMA);
    return () => db.close();
  });

  const dropped = {tag: 'drop-table', id: {schema: 'public', name: 'old'}};

  function insert(count: number, tag: string, change: string) {
    db.prepare(`
      INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
        ("watermark", "pos", "tag", "estimatedBytes", "change")
        WITH RECURSIVE n(i) AS (
          SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?
        )
        SELECT '05', i, ?, 0, ? FROM n
    `).run(count, tag, change);
  }

  test('schema lookup uses the partial index without sorting', () => {
    const plan = db
      .prepare('EXPLAIN QUERY PLAN ' + READ_SCHEMA_CHANGES_SQL)
      .all<{detail: string}>('03', '07')
      .map(row => row.detail)
      .join('\n');
    expect(plan).toContain(CHANGE_LOG_STREAM_FOLD_TAG_INDEX);
    expect(plan).not.toContain('TEMP B-TREE');
  });

  test('ordinary traffic does not consume the fold limit or get parsed', () => {
    insert(20_000, 'insert', 'not JSON');
    db.prepare(`INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
      ("watermark", "pos", "tag", "estimatedBytes", "change")
      VALUES ('06', 0, 'drop-table', 0, ?)`).run(JSON.stringify(dropped));
    expect(readSchemaChanges(db, '03', '07')).toEqual([dropped]);
    expect(readSchemaChanges(db, '03', '05')).toEqual([]);
    expect(readSchemaChanges(db, '06', '07')).toEqual([]);
  });

  test('only schema changes count toward the advisory comparison cap', () => {
    insert(MAX_FOLD_SCAN_ROWS, 'drop-table', JSON.stringify(dropped));
    expect(readSchemaChanges(db, '03', '07')).toHaveLength(MAX_FOLD_SCAN_ROWS);
    db.prepare(`INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
      ("watermark", "pos", "tag", "estimatedBytes", "change")
      VALUES ('06', 0, 'backfill-started', 0, 'not JSON')`).run();
    expect(readSchemaChanges(db, '03', '07')).toHaveLength(MAX_FOLD_SCAN_ROWS);
    db.prepare(`INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
      ("watermark", "pos", "tag", "estimatedBytes", "change")
      VALUES ('07', 0, 'drop-table', 0, ?)`).run(JSON.stringify(dropped));
    expect(readSchemaChanges(db, '03', '07')).toBeUndefined();
  });
});
