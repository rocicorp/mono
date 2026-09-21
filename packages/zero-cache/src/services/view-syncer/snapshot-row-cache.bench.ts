import {bench, describe, use} from '../../../../shared/src/bench.ts';
import {stringify} from '../../../../shared/src/bigint-json.ts';
import {getOrInsert, getOrInsertComputed} from '../../../../shared/src/map.ts';
import {SnapshotRowCache} from './snapshot-row-cache.ts';

// Compares ways of building the SnapshotRowCache entry key: the string key
// the cache uses today vs. folding (tag, sqlID, args) into an xxHash32-based
// digest the way `hashAST` does (zero-protocol/src/query-hash-visitor.ts).

type Cache = {
  getOrRead<T>(tag: string, sql: string, args: unknown[], read: () => T): T;
};

// ---------------------------------------------------------------------------
// Variant: string key (copy of SnapshotRowCache#key, without metrics).
// ---------------------------------------------------------------------------

function serialize(v: unknown): string {
  switch (typeof v) {
    case 'string':
      return 's' + v;
    case 'number':
    case 'bigint':
      return String(v);
    case 'boolean':
      return v ? 'true' : 'false';
    case 'object':
      return v === null ? 'null' : 'o' + stringify(v);
    default:
      return 'u' + String(v);
  }
}

class StringKeyCache implements Cache {
  readonly #entries = new Map<string, unknown>();
  readonly #sqlIDs = new Map<string, number>();

  getOrRead<T>(tag: string, sql: string, args: unknown[], read: () => T): T {
    const sqlID = getOrInsert(this.#sqlIDs, sql, this.#sqlIDs.size);
    let key = `${tag}\0${sqlID}`;
    for (const arg of args) {
      const serialized = serialize(arg);
      key += `\0${serialized.length}:${serialized}`;
    }
    const cached = this.#entries.get(key);
    if (cached !== undefined) {
      return cached as T;
    }
    const value = read();
    if (value !== undefined) {
      this.#entries.set(key, value);
    }
    return value;
  }
}

// ---------------------------------------------------------------------------
// Hashing, after query-hash-visitor.ts: two xxHash32 lanes in module lets.
// ---------------------------------------------------------------------------

// Local copies of shared/src/xxhash32.ts. vitest's module transform turns
// calls to imported functions into namespace property loads, which penalizes
// the per-word hot loop far more than the bundled production build would.
const PRIME32_1 = 2654435761;
const PRIME32_2 = 2246822519;
const PRIME32_3 = 3266489917;
const PRIME32_5 = 374761393;

function round32(acc: number, word: number): number {
  acc = (acc + Math.imul(word, PRIME32_2)) | 0;
  acc = (acc << 13) | (acc >>> 19);
  return Math.imul(acc, PRIME32_1);
}

function avalanche32(acc: number): number {
  acc ^= acc >>> 15;
  acc = Math.imul(acc, PRIME32_2);
  acc ^= acc >>> 13;
  acc = Math.imul(acc, PRIME32_3);
  acc ^= acc >>> 16;
  return acc >>> 0;
}

let h1 = 0;
let h2 = 0;

function mix(w: number): void {
  h1 = round32(h1, w);
  h2 = round32(h2, w);
}

const STR_MARK = 0x40000000;
const TAG_NULL = 0x1001;
const TAG_FALSE = 0x1002;
const TAG_TRUE = 0x1003;
const TAG_INT = 0x1004;
const TAG_FLOAT = 0x1005;
const TAG_INT53 = 0x1006;
const TAG_BIG = 0x1007;
const TAG_OBJ = 0x1008;
const TAG_UNDEF = 0x100a;

function mixString(s: string): void {
  const n = s.length;
  mix(n | STR_MARK);
  let i = 0;
  for (; i + 1 < n; i += 2) {
    mix(s.charCodeAt(i) | (s.charCodeAt(i + 1) << 16));
  }
  if (i < n) {
    mix(s.charCodeAt(i));
  }
}

const numView = new DataView(new ArrayBuffer(8));

// Numbers and bigints that denote the same integer must hash identically.
function mixInteger(n: number): void {
  if ((n | 0) === n) {
    mix(TAG_INT);
    mix(n);
  } else {
    mix(TAG_INT53);
    mix((n % 0x1_0000_0000) | 0);
    mix(Math.floor(n / 0x1_0000_0000) | 0);
  }
}

const MIN_SAFE = BigInt(Number.MIN_SAFE_INTEGER);
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);

function mixArg(v: unknown): void {
  switch (typeof v) {
    case 'string':
      mixString(v);
      return;
    case 'number':
      if (Number.isSafeInteger(v)) {
        mixInteger(v);
      } else {
        mix(TAG_FLOAT);
        numView.setFloat64(0, v, true);
        mix(numView.getInt32(0, true));
        mix(numView.getInt32(4, true));
      }
      return;
    case 'bigint':
      if (v >= MIN_SAFE && v <= MAX_SAFE) {
        mixInteger(Number(v));
      } else {
        mix(TAG_BIG);
        mixString(v.toString());
      }
      return;
    case 'boolean':
      mix(v ? TAG_TRUE : TAG_FALSE);
      return;
    case 'object':
      if (v === null) {
        mix(TAG_NULL);
      } else {
        mix(TAG_OBJ);
        mixString(stringify(v));
      }
      return;
    default:
      mix(TAG_UNDEF);
  }
}

function hashKey(tag: string, sqlID: number, args: unknown[]): void {
  h1 = PRIME32_5;
  h2 = PRIME32_1;
  mixString(tag);
  mix(sqlID);
  mix(args.length);
  for (let i = 0; i < args.length; i++) {
    mixArg(args[i]);
  }
}

/** 53-bit digest as a number: 32 bits of lane 1, 21 bits of lane 2. */
function numericDigest(): number {
  return avalanche32(h1) * 0x20_0000 + (avalanche32(h2) >>> 11);
}

/** 64-bit digest as a string, as hashAST's finalize() renders it. */
function stringDigest(): string {
  return (
    avalanche32(h1).toString(36).padStart(7, '0') +
    avalanche32(h2).toString(36).padStart(7, '0')
  );
}

class HashNumberKeyCache implements Cache {
  readonly #entries = new Map<number, unknown>();
  readonly #sqlIDs = new Map<string, number>();

  getOrRead<T>(tag: string, sql: string, args: unknown[], read: () => T): T {
    hashKey(tag, getOrInsert(this.#sqlIDs, sql, this.#sqlIDs.size), args);
    const key = numericDigest();
    const cached = this.#entries.get(key);
    if (cached !== undefined) {
      return cached as T;
    }
    const value = read();
    if (value !== undefined) {
      this.#entries.set(key, value);
    }
    return value;
  }
}

class HashStringKeyCache implements Cache {
  readonly #entries = new Map<string, unknown>();
  readonly #sqlIDs = new Map<string, number>();

  getOrRead<T>(tag: string, sql: string, args: unknown[], read: () => T): T {
    hashKey(tag, getOrInsert(this.#sqlIDs, sql, this.#sqlIDs.size), args);
    const key = stringDigest();
    const cached = this.#entries.get(key);
    if (cached !== undefined) {
      return cached as T;
    }
    const value = read();
    if (value !== undefined) {
      this.#entries.set(key, value);
    }
    return value;
  }
}

// Collision-safe: the entry remembers its inputs and a hit is only a hit if
// they match (a mismatch is treated as a miss and replaces the entry).
type Entry = {tag: string; sqlID: number; args: unknown[]; value: unknown};

function sameArg(a: unknown, b: unknown): boolean {
  if (a === b) {
    return true;
  }
  const ta = typeof a;
  const tb = typeof b;
  if (
    (ta === 'number' || ta === 'bigint') &&
    (tb === 'number' || tb === 'bigint')
  ) {
    // oxlint-disable-next-line eqeqeq
    return a == b;
  }
  return (
    ta === 'object' &&
    tb === 'object' &&
    a !== null &&
    b !== null &&
    stringify(a) === stringify(b)
  );
}

function sameArgs(a: unknown[], b: unknown[]): boolean {
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i++) {
    if (!sameArg(a[i], b[i])) {
      return false;
    }
  }
  return true;
}

class VerifiedHashNumberKeyCache implements Cache {
  readonly #entries = new Map<number, Entry>();
  readonly #sqlIDs = new Map<string, number>();

  getOrRead<T>(tag: string, sql: string, args: unknown[], read: () => T): T {
    const sqlID = getOrInsert(this.#sqlIDs, sql, this.#sqlIDs.size);
    hashKey(tag, sqlID, args);
    const key = numericDigest();
    const cached = this.#entries.get(key);
    if (
      cached !== undefined &&
      cached.tag === tag &&
      cached.sqlID === sqlID &&
      sameArgs(cached.args, args)
    ) {
      return cached.value as T;
    }
    const value = read();
    if (value !== undefined) {
      this.#entries.set(key, {tag, sqlID, args, value});
    }
    return value;
  }
}

// Floor: interns the SQL (as every variant does) but builds no key and
// never caches, so every read "misses". Shows the cost outside the key.
class SqlInternOnly implements Cache {
  readonly #sqlIDs = new Map<string, number>();

  getOrRead<T>(_tag: string, sql: string, _args: unknown[], read: () => T): T {
    use(getOrInsert(this.#sqlIDs, sql, this.#sqlIDs.size));
    return read();
  }
}

// ---------------------------------------------------------------------------
// Workload: G client groups each diff the same transaction of M row changes.
// Per change, as in Snapshotter's Diff: one read of the new value from `curr`
// (tag `n:<stateVersion>`, args are JSON numbers/strings from the change log
// row key) and one read of the previous value from `prev` (tag
// `p:<prevVersion>`, args come from the SQLite row: bigints for integers).
// The first group misses, the rest hit.
// ---------------------------------------------------------------------------

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
      // Tags are rebuilt per read, as the Snapshotter does. The SQL is either
      // rebuilt per read too (before) or memoized per table (after).
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
  ['string key (current)', () => new StringKeyCache()],
  ['hash → number key', () => new HashNumberKeyCache()],
  ['hash → base36 string key', () => new HashStringKeyCache()],
  ['hash → number key, verified', () => new VerifiedHashNumberKeyCache()],
  ['SnapshotRowCache (actual, with metrics)', () => new SnapshotRowCache()],
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

// Key construction alone (no Map), per read.
describe('key construction only (1000 keys, string PK)', () => {
  const changes = makeChanges(M, 'string');
  bench('string key', () => {
    let len = 0;
    for (const c of changes) {
      let key = `n:${c.stateVersion}\x000`;
      for (const arg of c.nextArgs) {
        const s = serialize(arg);
        key += `\0${s.length}:${s}`;
      }
      len += key.length;
    }
    use(len);
  });
  bench('hash → number', () => {
    let x = 0;
    for (const c of changes) {
      hashKey(`n:${c.stateVersion}`, 0, c.nextArgs);
      x += numericDigest();
    }
    use(x);
  });
  bench('hash → base36 string', () => {
    let len = 0;
    for (const c of changes) {
      hashKey(`n:${c.stateVersion}`, 0, c.nextArgs);
      len += stringDigest().length;
    }
    use(len);
  });
});
