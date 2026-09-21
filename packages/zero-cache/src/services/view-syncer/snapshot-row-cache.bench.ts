import {bench, describe, use} from '../../../../shared/src/bench.ts';
import {getOrInsert, getOrInsertComputed} from '../../../../shared/src/map.ts';
import {SnapshotRowCache} from './snapshot-row-cache.ts';

// Measures the per-read overhead of the SnapshotRowCache (i.e. everything
// but the SQLite reads themselves) on the access pattern of Snapshotter's
// Diff, with the lookup SQL either rebuilt for every read or memoized per
// table (as the Snapshotter does).

type Cache = {
  getOrRead<T>(tag: string, sql: string, args: unknown[], read: () => T): T;
};

// Floor: interns the SQL (as the cache does) but builds no key and never
// caches, so every read "misses". Shows the cost outside of the cache key.
class SqlInternOnly implements Cache {
  readonly #sqlIDs = new Map<string, number>();

  getOrRead<T>(_tag: string, sql: string, _args: unknown[], read: () => T): T {
    use(getOrInsert(this.#sqlIDs, sql, this.#sqlIDs.size));
    return read();
  }
}

// Workload: G client groups each diff the same transaction of M row changes.
// Per change, as in Snapshotter's Diff: one read of the new value from `curr`
// (tag `n:<stateVersion>`, args are JSON numbers/strings from the change log
// row key) and one read of the previous value from `prev` (tag
// `p:<prevVersion>`, args come from the SQLite row: bigints for integers).
// The first group misses, the rest hit.

const PREFIX =
  'SELECT "id","title","description","created","modified","creatorID","assigneeID","open","visibility","_0_version" FROM "issue" WHERE ';

type Change = {
  stateVersion: string;
  keyCol: string;
  nextArgs: unknown[];
  prevArgs: unknown[];
};

function randomID(i: number): string {
  // nanoid-like 21 char ids.
  return (i.toString(36) + 'Xy7_kQpLm2ZrT9bWc4dFe').slice(0, 21);
}

function makeChanges(m: number, keyType: 'string' | 'int'): Change[] {
  const changes: Change[] = [];
  for (let i = 0; i < m; i++) {
    const stateVersion = `1a2b3c${(i >> 4).toString(36).padStart(4, '0')}`;
    if (keyType === 'string') {
      const id = randomID(i);
      changes.push({
        stateVersion,
        keyCol: 'id',
        nextArgs: [id],
        prevArgs: [id],
      });
    } else {
      const id = 1_000_000 + i;
      changes.push({
        stateVersion,
        keyCol: 'id',
        nextArgs: [id],
        prevArgs: [BigInt(id)],
      });
    }
  }
  return changes;
}

const ROW = {id: 'x', title: 'hello'};
const ROWS = [ROW];
const readRow = () => ROW;
const readRows = () => ROWS;

// Stand-in for the Snapshotter's per-table SQL memo: the same string object
// for every read of a given shape.
const memoizedSQL = new Map<string, string>();

function runWorkload(
  cache: Cache,
  changes: Change[],
  groups: number,
  stableSQL: boolean,
): number {
  let n = 0;
  const prevTag = `p:1a2b3b0000`;
  for (let g = 0; g < groups; g++) {
    for (const c of changes) {
      // Tags are rebuilt per read, as the Snapshotter does.
      const sql = stableSQL
        ? getOrInsertComputed(
            memoizedSQL,
            c.keyCol,
            () => PREFIX + `"${c.keyCol}"=?`,
          )
        : PREFIX + `"${c.keyCol}"=?`;
      const next = cache.getOrRead(
        `n:${c.stateVersion}`,
        sql,
        c.nextArgs,
        readRow,
      );
      const prev = cache.getOrRead(prevTag, sql, c.prevArgs, readRows);
      n += (next ? 1 : 0) + prev.length;
    }
  }
  return n;
}

const M = 1_000;
const G = 20;

const variants: [string, () => Cache][] = [
  ['SnapshotRowCache', () => new SnapshotRowCache()],
  ['floor: SQL intern only, no key/cache', () => new SqlInternOnly()],
];

for (const keyType of ['string', 'int'] as const) {
  describe(`snapshot row cache: ${G} groups × ${M} changes × 2 reads, ${keyType} PK`, () => {
    const changes = makeChanges(M, keyType);
    for (const [name, make] of variants) {
      bench(`${name}, SQL rebuilt per read`, () => {
        use(runWorkload(make(), changes, G, false));
      });
      bench(`${name}, SQL memoized`, () => {
        use(runWorkload(make(), changes, G, true));
      });
    }
  });
}
