import type {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import {computeZqlSpecs} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../db/specs.ts';
import {DbFile} from '../../test/lite.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.ts';
import {
  SharedDiffs,
  specsFingerprint,
  type SharedDiffsOptions,
} from './shared-diffs.ts';
import {
  ResetPipelinesSignal,
  Snapshotter,
  type Change,
  type SnapshotDiff,
} from './snapshotter.ts';

describe('view-syncer/shared-diffs', () => {
  let lc: LogContext;
  let dbFile: DbFile;
  let replicator: FakeReplicator;
  let tableSpecs: Map<string, LiteAndZqlSpec>;
  let allTableNames: Set<string>;
  let fingerprint: string;
  const toDestroy: {destroy(): void}[] = [];

  beforeEach(() => {
    lc = createSilentLogContext();
    dbFile = new DbFile('shared_diffs_test');
    const db = dbFile.connect(lc);
    db.pragma('journal_mode = WAL2');
    db.exec(/*sql*/ `
      CREATE TABLE "my_app.permissions" (
        "lock"        INT PRIMARY KEY,
        "permissions" JSON,
        "hash"        TEXT,
        _0_version    TEXT NOT NULL
      );
      INSERT INTO "my_app.permissions" ("lock", "_0_version") VALUES (1, '01');
      CREATE TABLE issues(id INT PRIMARY KEY, owner INTEGER, desc TEXT, _0_version TEXT NOT NULL);
      CREATE TABLE users(id INT PRIMARY KEY, handle TEXT UNIQUE, _0_version TEXT NOT NULL);

      INSERT INTO issues(id, owner, desc, _0_version) VALUES(1, 10, 'foo', '01');
      INSERT INTO issues(id, owner, desc, _0_version) VALUES(2, 10, 'bar', '01');
      INSERT INTO users(id, handle, _0_version) VALUES(10, 'alice', '01');
      INSERT INTO users(id, handle, _0_version) VALUES(20, 'bob', '01');
    `);
    initReplicationState(db, ['zero_data'], '01');
    tableSpecs = computeZqlSpecs(lc, db, {includeBackfillingColumns: false});
    allTableNames = new Set(tableSpecs.keys());
    fingerprint = specsFingerprint(tableSpecs);
    replicator = fakeReplicator(lc, db);
  });

  afterEach(() => {
    for (const d of toDestroy.splice(0)) {
      d.destroy();
    }
    dbFile.delete();
  });

  const messages = new ReplicationMessages({
    'issues': 'id',
    'users': 'id',
    ['my_app.permissions']: 'lock',
  });

  function sharedDiffs(options: Partial<SharedDiffsOptions> = {}) {
    const shared = new SharedDiffs(
      lc,
      dbFile.path,
      {appID: 'my_app'},
      {
        maxBytes: 1024 * 1024,
        ...options,
      },
    );
    toDestroy.push(shared);
    return shared;
  }

  function snapshotter(shared?: SharedDiffs) {
    const s = new Snapshotter(
      lc,
      dbFile.path,
      {appID: 'my_app'},
      undefined,
      undefined,
      shared,
    ).init();
    toDestroy.push(s);
    return s;
  }

  function advance(s: Snapshotter, prevWrites: 'none' | 'divergent' = 'none') {
    return s.advance(
      tableSpecs,
      allTableNames,
      undefined,
      prevWrites,
      fingerprint,
    );
  }

  /** Whether the diff replays segments rather than computing its changes. */
  function isShared(diff: SnapshotDiff) {
    return diff.constructor.name === 'SegmentDiff';
  }

  /** Applies `changes` to a copy of `rows`, keyed by table and id. */
  function apply(
    rows: ReadonlyMap<string, Row>,
    changes: Iterable<Change>,
  ): Map<string, Row> {
    const result = new Map(rows);
    for (const {table, prevValues, nextValue} of changes) {
      for (const prev of prevValues) {
        result.delete(`${table}/${String(prev.id)}`);
      }
      if (nextValue) {
        result.set(`${table}/${String(nextValue.id)}`, nextValue);
      }
    }
    return result;
  }

  test('a client group at the versions of the producer replays its segment', () => {
    const shared = sharedDiffs();
    const a = snapshotter(shared);
    const b = snapshotter(shared);
    const own = snapshotter();
    // Starts the producer at 01.
    expect(isShared(advance(a))).toBe(false);

    replicator.processTransaction(
      '02',
      messages.insert('issues', {id: 3, owner: 20, desc: 'baz'}),
      messages.update('users', {id: 20, handle: 'robert'}),
    );

    const diffA = advance(a);
    const diffB = advance(b);
    const diffOwn = advance(own);
    expect(isShared(diffA)).toBe(true);
    expect(isShared(diffB)).toBe(true);
    expect(isShared(diffOwn)).toBe(false);
    expect(diffA.rowsMayRepeat).toBe(false);
    expect(diffA.changes).toBe(2);

    const changesA = [...diffA];
    expect(changesA).toEqual([...diffOwn]);
    // The client groups share the rows.
    const changesB = [...diffB];
    expect(changesB[0].nextValue).toBe(changesA[0].nextValue);
  });

  test('a client group that fell behind replays several segments in order', () => {
    const shared = sharedDiffs();
    const behind = snapshotter(shared);
    const keepingUp = snapshotter(shared);
    const own = snapshotter();
    advance(keepingUp);

    const initial = new Map<string, Row>();
    const versions = ['02', '03', '04'];
    for (const [i, version] of versions.entries()) {
      replicator.processTransaction(
        version,
        messages.update('issues', {id: 1, owner: 10, desc: `foo${i}`}),
        messages.update('users', {id: 10, handle: `alice${i}`}),
        ...(i === 1
          ? [messages.delete('issues', {id: 2})]
          : [messages.insert('issues', {id: 5 + i, owner: 20, desc: 'new'})]),
      );
      expect(isShared(advance(keepingUp))).toBe(true);
    }

    const diff = advance(behind);
    expect(isShared(diff)).toBe(true);
    expect(diff.rowsMayRepeat).toBe(true);
    const diffOwn = advance(own);
    expect(diff.changes).toBeGreaterThan(diffOwn.changes);
    expect(apply(initial, diff)).toEqual(apply(initial, diffOwn));
  });

  test('client groups that converted rows with other specs compute their own diffs', () => {
    const shared = sharedDiffs();
    const a = snapshotter(shared);
    advance(a);
    replicator.processTransaction(
      '02',
      messages.insert('issues', {id: 3, owner: 20, desc: 'baz'}),
    );
    const diff = a.advance(
      tableSpecs,
      allTableNames,
      undefined,
      'none',
      'other specs',
    );
    expect(isShared(diff)).toBe(false);
  });

  test('a client group whose version the producer did not advance to computes its own diff', () => {
    const shared = sharedDiffs();
    const producerStarter = snapshotter(shared);
    advance(producerStarter);
    replicator.processTransaction(
      '02',
      messages.insert('issues', {id: 3, owner: 20, desc: 'baz'}),
    );
    // Advances to 02 without the producer.
    const a = snapshotter(shared);
    replicator.processTransaction(
      '03',
      messages.insert('issues', {id: 4, owner: 20, desc: 'qux'}),
    );
    expect(isShared(advance(a))).toBe(false);
    // It is now at a version of the producer.
    replicator.processTransaction(
      '04',
      messages.insert('issues', {id: 5, owner: 20, desc: 'quux'}),
    );
    expect(isShared(advance(a))).toBe(true);
  });

  test('segments held by a client group count until it is done with them', () => {
    const shared = sharedDiffs();
    const a = snapshotter(shared);
    advance(a);
    replicator.processTransaction(
      '02',
      messages.insert('issues', {id: 3, owner: 20, desc: 'baz'}),
    );
    const diff = advance(a);
    expect(isShared(diff)).toBe(true);
    const held = shared.liveBytes;
    expect(held).toBeGreaterThan(0);
    // The diff is never iterated: the next advance releases it, and as `a`
    // is past it, its segment is dropped, leaving the new one (of the same
    // size).
    replicator.processTransaction(
      '03',
      messages.insert('issues', {id: 4, owner: 20, desc: 'qux'}),
    );
    expect(isShared(advance(a))).toBe(true);
    expect(shared.liveBytes).toBe(held);
  });

  test('segments that every client group has advanced past are dropped', () => {
    const shared = sharedDiffs();
    const a = snapshotter(shared);
    const b = snapshotter(shared);
    advance(a);
    advance(b);
    const versions = ['02', '03', '04'];
    for (const version of versions) {
      replicator.processTransaction(
        version,
        messages.insert('issues', {id: Number(version), owner: 20, desc: 'x'}),
      );
      expect(isShared(advance(a))).toBe(true);
    }
    // `b` is still at 01, so every segment is kept.
    const allSegments = shared.liveBytes;
    // It advances across all of them, and is then done with them.
    const diff = advance(b);
    expect(isShared(diff)).toBe(true);
    expect([...diff]).toHaveLength(3);
    replicator.processTransaction(
      '05',
      messages.insert('issues', {id: 5, owner: 20, desc: 'x'}),
    );
    expect(isShared(advance(a))).toBe(true);
    // Both are at 04 or later, so only the segment from 04 is kept.
    expect(shared.liveBytes).toBeLessThan(allSegments / 2);

    // Without client groups, nothing is kept.
    a.destroy();
    b.destroy();
    const c = snapshotter(shared);
    replicator.processTransaction(
      '06',
      messages.insert('issues', {id: 6, owner: 20, desc: 'x'}),
    );
    advance(c);
    expect(shared.liveBytes).toBeLessThan(allSegments / 2);
  });

  test('advances that do not fit the budget are not shared', () => {
    const shared = sharedDiffs({maxBytes: 1500, maxSegmentBytes: 1500});
    const a = snapshotter(shared);
    const b = snapshotter(shared);
    advance(a);
    advance(b);

    replicator.processTransaction(
      '02',
      messages.insert('issues', {id: 3, owner: 20, desc: 'x'.repeat(200)}),
    );
    const held = advance(a);
    expect(isShared(held)).toBe(true);
    const bytes = shared.liveBytes;
    expect(bytes).toBeLessThanOrEqual(1500);

    // `a` holds the first segment, so a second one of the same size does
    // not fit, and neither client group can replay the range.
    replicator.processTransaction(
      '03',
      messages.insert('issues', {id: 4, owner: 20, desc: 'y'.repeat(200)}),
    );
    const diffB = advance(b);
    expect(isShared(diffB)).toBe(false);
    expect(shared.liveBytes).toBe(bytes);

    // Once `a` is done, the first segment is released: it had already left
    // the ring to make room.
    expect([...held]).toHaveLength(1);
    expect(shared.liveBytes).toBe(0);
    expect(isShared(advance(a))).toBe(false);
  });

  test('an advance with too many changes is not shared', () => {
    const shared = sharedDiffs({maxSegmentChanges: 1});
    const a = snapshotter(shared);
    advance(a);
    replicator.processTransaction(
      '02',
      messages.insert('issues', {id: 3, owner: 20, desc: 'baz'}),
      messages.insert('issues', {id: 4, owner: 20, desc: 'qux'}),
    );
    expect(isShared(advance(a))).toBe(false);
    expect(shared.liveBytes).toBe(0);
  });

  test('an idle producer closes its snapshots and starts again at head', () => {
    vi.useFakeTimers();
    try {
      const shared = sharedDiffs({idleMs: 1000});
      const a = snapshotter(shared);
      const b = snapshotter(shared);
      advance(a);
      advance(b);
      replicator.processTransaction(
        '02',
        messages.insert('issues', {id: 3, owner: 20, desc: 'baz'}),
      );
      expect(isShared(advance(a))).toBe(true);

      // Neither client group advances for a while.
      vi.advanceTimersByTime(2000);
      replicator.processTransaction(
        '03',
        messages.insert('issues', {id: 4, owner: 20, desc: 'qux'}),
      );
      // The producer starts again at 03, so `b` (at 01) and `a` (at 02)
      // compute their own diffs...
      expect(isShared(advance(b))).toBe(false);
      expect(isShared(advance(a))).toBe(false);
      // ...until they are at a version it advanced to.
      replicator.processTransaction(
        '04',
        messages.insert('issues', {id: 5, owner: 20, desc: 'quux'}),
      );
      expect(isShared(advance(a))).toBe(true);
      expect(isShared(advance(b))).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  test('a truncate is not shared, so that each client group resets', () => {
    const shared = sharedDiffs();
    const a = snapshotter(shared);
    advance(a);
    replicator.processTransaction('02', messages.truncate('issues'));
    const diff = advance(a);
    expect(isShared(diff)).toBe(false);
    expect(() => [...diff]).toThrow(ResetPipelinesSignal);

    // Sharing resumes with the next advance.
    replicator.processTransaction(
      '03',
      messages.insert('issues', {id: 3, owner: 20, desc: 'baz'}),
    );
    expect(isShared(advance(a))).toBe(true);
  });

  test('a client group that writes to prev reads the rows a change displaces from it', () => {
    const shared = sharedDiffs();
    const a = snapshotter(shared);
    const b = snapshotter(shared);
    advance(a);
    advance(b);
    // Takes bob's id and alice's handle.
    replicator.processTransaction(
      '02',
      messages.insert('users', {id: 20, handle: 'alice'}),
    );

    const segment = advance(a, 'none');
    const displaced = [...segment].flatMap(c => c.prevValues.map(r => r.id));
    expect(displaced.sort()).toEqual([10, 20]);

    // `b` writes to its `prev`, where an earlier change removed alice.
    const writing = advance(b, 'divergent');
    expect(isShared(writing)).toBe(true);
    writing.prev.db.run('DELETE FROM users WHERE id = 10');
    const reread = [...writing].flatMap(c => c.prevValues.map(r => r.id));
    expect(reread).toEqual([20]);
  });
});
