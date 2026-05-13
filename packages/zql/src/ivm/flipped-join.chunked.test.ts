import {afterEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {CaughtNode} from './catch.ts';
import {Catch} from './catch.ts';
import type {Node} from './data.ts';
import {
  canonicalKeyForTest,
  FlippedJoin,
  setMultiConstraintChunkSizeForTest,
} from './flipped-join.ts';
import type {FetchRequest, Input, Output} from './operator.ts';
import {Snitch, type FetchMessage, type SnitchMessage} from './snitch.ts';
import {makeSourceChangeAdd, makeSourceChangeRemove} from './source.ts';
import type {Stream} from './stream.ts';
import {consume} from './stream.ts';
import {createSource} from './test/source-factory.ts';

type CaughtRow = Exclude<CaughtNode, 'yield'>;

function asRow(n: CaughtNode): CaughtRow {
  if (n === 'yield') {
    throw new Error('unexpected yield in catch result');
  }
  return n;
}

function parentFetchMessages(log: readonly SnitchMessage[]): FetchMessage[] {
  return log.filter(
    (msg): msg is FetchMessage => msg[0] === 'p' && msg[1] === 'fetch',
  );
}

const lc = createSilentLogContext();

let restoreChunkSize: (() => void) | undefined;

afterEach(() => {
  restoreChunkSize?.();
  restoreChunkSize = undefined;
});

/**
 * When `multiConstraint` exceeds the chunk size, FlippedJoin issues
 * multiple parent.fetch calls and merges their sorted streams. This test
 * sets the chunk size to 2 with 5 children so we get 3 chunks (2 + 2 + 1)
 * and asserts:
 *   - the parent fetches are split across 3 calls
 *   - the merged output is in parent compareRows order
 *   - each parent is grouped with the right children
 */
test('chunked fetch merges sorted streams across multiple parent.fetch calls', () => {
  // Force a tiny chunk so a moderate child count exercises the path.
  restoreChunkSize = setMultiConstraintChunkSizeForTest(2);

  const parent = createSource(
    lc,
    testLogConfig,
    'parent',
    {id: {type: 'string'}, label: {type: 'string'}},
    ['id'],
  );
  const child = createSource(
    lc,
    testLogConfig,
    'child',
    {id: {type: 'string'}, parentId: {type: 'string'}},
    ['id'],
  );

  // 5 distinct parents, 5 children referencing 5 distinct parent ids.
  for (let i = 1; i <= 5; i++) {
    consume(
      parent.push(makeSourceChangeAdd({id: `p${i}`, label: `Parent ${i}`})),
    );
  }
  for (let i = 1; i <= 5; i++) {
    consume(child.push(makeSourceChangeAdd({id: `c${i}`, parentId: `p${i}`})));
  }

  const log: SnitchMessage[] = [];
  const parentSnitch = new Snitch(parent.connect([['id', 'asc']]), 'p', log);
  const childSnitch = new Snitch(child.connect([['id', 'asc']]), 'c', log);

  const fj = new FlippedJoin({
    parent: parentSnitch,
    child: childSnitch,
    parentKey: ['id'],
    childKey: ['parentId'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });

  const result = new Catch(fj).fetch({});

  // 5 parents, in id order, each with the matching child grouped under it.
  expect(result.map(n => asRow(n).row.id)).toEqual([
    'p1',
    'p2',
    'p3',
    'p4',
    'p5',
  ]);
  for (const node of result) {
    const row = asRow(node);
    const children = row.relationships.children;
    expect(children).toHaveLength(1);
    expect(asRow(children[0]).row.parentId).toBe(row.row.id);
  }

  // 1 child fetch, then 3 parent fetches (chunks of 2, 2, 1).
  const parentFetches = parentFetchMessages(log);
  expect(parentFetches).toHaveLength(3);
  // Each parent fetch carries one multiConstraints entry (FlippedJoin's
  // computed chunk), capped at the chunk size.
  expect(must(parentFetches[0][2].multiConstraints)[0]).toHaveLength(2);
  expect(must(parentFetches[1][2].multiConstraints)[0]).toHaveLength(2);
  expect(must(parentFetches[2][2].multiConstraints)[0]).toHaveLength(1);
});

test('single chunk path used when multiConstraints fits in one chunk', () => {
  // Default chunk size is 256; 3 children fit comfortably.
  const parent = createSource(
    lc,
    testLogConfig,
    'parent',
    {id: {type: 'string'}, label: {type: 'string'}},
    ['id'],
  );
  const child = createSource(
    lc,
    testLogConfig,
    'child',
    {id: {type: 'string'}, parentId: {type: 'string'}},
    ['id'],
  );

  for (let i = 1; i <= 3; i++) {
    consume(
      parent.push(makeSourceChangeAdd({id: `p${i}`, label: `Parent ${i}`})),
    );
  }
  for (let i = 1; i <= 3; i++) {
    consume(child.push(makeSourceChangeAdd({id: `c${i}`, parentId: `p${i}`})));
  }

  const log: SnitchMessage[] = [];
  const parentSnitch = new Snitch(parent.connect([['id', 'asc']]), 'p', log);
  const childSnitch = new Snitch(child.connect([['id', 'asc']]), 'c', log);

  const fj = new FlippedJoin({
    parent: parentSnitch,
    child: childSnitch,
    parentKey: ['id'],
    childKey: ['parentId'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });

  const result = new Catch(fj).fetch({});
  expect(result.map(n => asRow(n).row.id)).toEqual(['p1', 'p2', 'p3']);

  const parentFetches = parentFetchMessages(log);
  expect(parentFetches).toHaveLength(1);
  expect(must(parentFetches[0][2].multiConstraints)[0]).toHaveLength(3);
});

test('chunked fetch propagates .return() to sub-streams on early termination', () => {
  // Regression: previously mergeSortedStreams had no try/finally. When a
  // downstream consumer (e.g. Take after limit is reached) called
  // .return() mid-merge, sub-stream iterators leaked. With SQLite cursors
  // backing those iterators, this caused subsequent operations on the
  // same connection to fail with "database connection is busy".
  restoreChunkSize = setMultiConstraintChunkSizeForTest(2);

  const parent = createSource(
    lc,
    testLogConfig,
    'parent',
    {id: {type: 'string'}, label: {type: 'string'}},
    ['id'],
  );
  const child = createSource(
    lc,
    testLogConfig,
    'child',
    {id: {type: 'string'}, parentId: {type: 'string'}},
    ['id'],
  );

  for (let i = 1; i <= 5; i++) {
    consume(
      parent.push(makeSourceChangeAdd({id: `p${i}`, label: `Parent ${i}`})),
    );
  }
  for (let i = 1; i <= 5; i++) {
    consume(child.push(makeSourceChangeAdd({id: `c${i}`, parentId: `p${i}`})));
  }

  // Wrap parent.fetch so each returned stream tracks whether .return()
  // was called. With chunk size 2 and 5 children we get 3 chunks; the
  // outer iterator only consumes from the first chunk before breaking,
  // so the second & third chunks must have .return() propagated.
  const parentInput = parent.connect([['id', 'asc']]);
  const returnCalls: number[] = [];
  let nextStreamIdx = 0;
  const wrappedParent: Input = {
    getSchema: () => parentInput.getSchema(),
    setOutput: (o: Output) => parentInput.setOutput(o),
    destroy: () => parentInput.destroy(),
    fetch: (req: FetchRequest): Stream<Node | 'yield'> => {
      const idx = nextStreamIdx++;
      const inner = parentInput.fetch(req);
      return {
        [Symbol.iterator]() {
          const it = inner[Symbol.iterator]();
          const wrapped: IterableIterator<Node | 'yield'> = {
            next: () => it.next(),
            return(value?: unknown): IteratorResult<Node | 'yield'> {
              returnCalls.push(idx);
              return it.return?.(value) ?? {done: true, value: undefined};
            },
            [Symbol.iterator]() {
              return wrapped;
            },
          };
          return wrapped;
        },
      };
    },
  };

  const fj = new FlippedJoin({
    parent: wrappedParent,
    child: child.connect([['id', 'asc']]),
    parentKey: ['id'],
    childKey: ['parentId'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });

  // Manually pull from the generator and break early, so .return() is
  // invoked (the for-of doesn't optimize this away).
  const stream = fj.fetch({});
  const it = stream[Symbol.iterator]();
  const first = it.next();
  expect(first.done).toBe(false);
  // Early termination — JS calls it.return() under the hood for break in
  // a for-of, but here we invoke it manually.
  it.return?.();

  // 3 chunks → 3 sub-streams. The first one we partially consumed; the
  // remaining 2 were primed but not advanced past their first row. All
  // 3 should have .return() called via the merge's finally block.
  expect(returnCalls.sort()).toEqual([0, 1, 2]);
});

test('chunked fetch forwards yields from parent and child sub-streams', () => {
  // The Input contract requires any 'yield' produced upstream to be
  // forwarded to the caller. For the chunked path, this means
  // `mergeSortedStreams` must forward yields from each sub-stream, and
  // FlippedJoin must forward them after the merge.
  restoreChunkSize = setMultiConstraintChunkSizeForTest(2);

  const parent = createSource(
    lc,
    testLogConfig,
    'parent',
    {id: {type: 'string'}, label: {type: 'string'}},
    ['id'],
  );
  const child = createSource(
    lc,
    testLogConfig,
    'child',
    {id: {type: 'string'}, parentId: {type: 'string'}},
    ['id'],
  );

  for (let i = 1; i <= 5; i++) {
    consume(
      parent.push(makeSourceChangeAdd({id: `p${i}`, label: `Parent ${i}`})),
    );
    consume(child.push(makeSourceChangeAdd({id: `c${i}`, parentId: `p${i}`})));
  }

  // Inject 'yield' before every row of every fetch on this Input.
  function yieldInjector(inner: Input): Input {
    return {
      getSchema: () => inner.getSchema(),
      setOutput: (o: Output) => inner.setOutput(o),
      destroy: () => inner.destroy(),
      *fetch(req: FetchRequest): Stream<Node | 'yield'> {
        for (const node of inner.fetch(req)) {
          yield 'yield';
          yield node;
        }
      },
    };
  }

  const fj = new FlippedJoin({
    parent: yieldInjector(parent.connect([['id', 'asc']])),
    child: yieldInjector(child.connect([['id', 'asc']])),
    parentKey: ['id'],
    childKey: ['parentId'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });

  // Collect both yields and rows so we can prove yields are forwarded.
  const yieldsAndRows: ('yield' | string)[] = [];
  for (const node of fj.fetch({})) {
    yieldsAndRows.push(node === 'yield' ? 'yield' : String(node.row.id));
  }

  // Rows are still produced in parent compareRows order...
  expect(yieldsAndRows.filter(x => x !== 'yield')).toEqual([
    'p1',
    'p2',
    'p3',
    'p4',
    'p5',
  ]);

  // ...and yields are forwarded. 5 from the child fetch + ≥5 from parent
  // chunks (one per row yielded by the wrapped parent.fetch in each of
  // the 3 chunks). The exact parent count depends on when the merge
  // primes vs. emits, so just assert there's a healthy number.
  const yieldCount = yieldsAndRows.filter(x => x === 'yield').length;
  expect(yieldCount).toBeGreaterThanOrEqual(10);
});

test('chunked fetch dedupes children sharing the same parent-key value', () => {
  restoreChunkSize = setMultiConstraintChunkSizeForTest(2);

  const parent = createSource(
    lc,
    testLogConfig,
    'parent',
    {id: {type: 'string'}, label: {type: 'string'}},
    ['id'],
  );
  const child = createSource(
    lc,
    testLogConfig,
    'child',
    {id: {type: 'string'}, parentId: {type: 'string'}},
    ['id'],
  );

  // 3 distinct parents but 6 children (each parent has 2 children).
  for (let i = 1; i <= 3; i++) {
    consume(
      parent.push(makeSourceChangeAdd({id: `p${i}`, label: `Parent ${i}`})),
    );
  }
  let n = 1;
  for (let i = 1; i <= 3; i++) {
    consume(
      child.push(makeSourceChangeAdd({id: `c${n++}`, parentId: `p${i}`})),
    );
    consume(
      child.push(makeSourceChangeAdd({id: `c${n++}`, parentId: `p${i}`})),
    );
  }

  const log: SnitchMessage[] = [];
  const parentSnitch = new Snitch(parent.connect([['id', 'asc']]), 'p', log);
  const childSnitch = new Snitch(child.connect([['id', 'asc']]), 'c', log);

  const fj = new FlippedJoin({
    parent: parentSnitch,
    child: childSnitch,
    parentKey: ['id'],
    childKey: ['parentId'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });

  const result = new Catch(fj).fetch({});

  // 3 parents, each grouped with its 2 children.
  expect(result).toHaveLength(3);
  for (const node of result) {
    expect(asRow(node).relationships.children).toHaveLength(2);
  }

  // Multi-constraint should have 3 unique entries (one per parent), not 6.
  // With chunk size 2 that becomes 2 chunks (2 + 1), not 3.
  const parentFetches = parentFetchMessages(log);
  expect(parentFetches).toHaveLength(2);
  expect(must(parentFetches[0][2].multiConstraints)[0]).toHaveLength(2);
  expect(must(parentFetches[1][2].multiConstraints)[0]).toHaveLength(1);
});

/**
 * Helper: 5 parents, 5 1:1 children, chunk size set to 2 → 3 chunks.
 * Returns the FlippedJoin and a setup that the FetchRequest tests below
 * share, so each test focuses on its own assertion. Each parent also
 * carries an `active` flag (p2 is inactive) so we can test req.constraint
 * on a non-join column.
 */
function setupFiveOneToOne() {
  restoreChunkSize = setMultiConstraintChunkSizeForTest(2);

  const parent = createSource(
    lc,
    testLogConfig,
    'parent',
    {
      id: {type: 'string'},
      label: {type: 'string'},
      active: {type: 'boolean'},
    },
    ['id'],
  );
  const child = createSource(
    lc,
    testLogConfig,
    'child',
    {id: {type: 'string'}, parentId: {type: 'string'}},
    ['id'],
  );

  for (let i = 1; i <= 5; i++) {
    consume(
      parent.push(
        makeSourceChangeAdd({
          id: `p${i}`,
          label: `Parent ${i}`,
          active: i !== 2,
        }),
      ),
    );
  }
  for (let i = 1; i <= 5; i++) {
    consume(child.push(makeSourceChangeAdd({id: `c${i}`, parentId: `p${i}`})));
  }

  const log: SnitchMessage[] = [];
  const parentSnitch = new Snitch(parent.connect([['id', 'asc']]), 'p', log);
  const childSnitch = new Snitch(child.connect([['id', 'asc']]), 'c', log);

  const fj = new FlippedJoin({
    parent: parentSnitch,
    child: childSnitch,
    parentKey: ['id'],
    childKey: ['parentId'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });

  return {fj, log};
}

test('chunked fetch with reverse: true yields parents in descending order', () => {
  const {fj, log} = setupFiveOneToOne();

  const result = new Catch(fj).fetch({reverse: true});
  expect(result.map(n => asRow(n).row.id)).toEqual([
    'p5',
    'p4',
    'p3',
    'p2',
    'p1',
  ]);
  // Each parent still has its own child grouped under it.
  for (const node of result) {
    const row = asRow(node);
    const children = row.relationships.children;
    expect(children).toHaveLength(1);
    expect(asRow(children[0]).row.parentId).toBe(row.row.id);
  }

  // 3 chunks (2+2+1), each carrying reverse: true.
  const parentFetches = parentFetchMessages(log);
  expect(parentFetches).toHaveLength(3);
  for (const f of parentFetches) {
    expect(f[2].reverse).toBe(true);
  }
});

test('chunked fetch with start basis "at" includes start row and continues forward', () => {
  const {fj, log} = setupFiveOneToOne();

  const result = new Catch(fj).fetch({start: {row: {id: 'p3'}, basis: 'at'}});
  expect(result.map(n => asRow(n).row.id)).toEqual(['p3', 'p4', 'p5']);

  // Each chunk's parent.fetch should carry the start parameter through.
  // Chunk 1 ([p1,p2]) should yield 0 rows since both are < p3; chunk 2
  // ([p3,p4]) yields p3,p4; chunk 3 ([p5]) yields p5.
  const parentFetches = parentFetchMessages(log);
  expect(parentFetches).toHaveLength(3);
  for (const f of parentFetches) {
    expect(f[2].start).toEqual({row: {id: 'p3'}, basis: 'at'});
  }
});

test('chunked fetch with start basis "after" excludes start row', () => {
  const {fj} = setupFiveOneToOne();

  const result = new Catch(fj).fetch({
    start: {row: {id: 'p3'}, basis: 'after'},
  });
  expect(result.map(n => asRow(n).row.id)).toEqual(['p4', 'p5']);
});

test('chunked fetch with start + reverse', () => {
  const {fj} = setupFiveOneToOne();

  const result = new Catch(fj).fetch({
    start: {row: {id: 'p3'}, basis: 'at'},
    reverse: true,
  });
  expect(result.map(n => asRow(n).row.id)).toEqual(['p3', 'p2', 'p1']);
});

test('chunked fetch with req.constraint on non-join column filters parents', () => {
  const {fj, log} = setupFiveOneToOne();

  // `active` is not in parentKey, so FlippedJoin can't translate it to a
  // child constraint — children are fetched unconstrained, all 5 land in
  // the multi, and the constraint rides along with each chunk's parent.fetch
  // for the source to apply alongside the IN list. p2 is inactive.
  const result = new Catch(fj).fetch({constraint: {active: true}});
  expect(result.map(n => asRow(n).row.id)).toEqual(['p1', 'p3', 'p4', 'p5']);

  // 3 chunks (5 unique parent ids ÷ chunk 2), each carrying the
  // constraint.
  const parentFetches = parentFetchMessages(log);
  expect(parentFetches).toHaveLength(3);
  for (const f of parentFetches) {
    expect(f[2].constraint).toEqual({active: true});
  }
});

describe('canonicalKey', () => {
  // canonicalKey backs the multi-IN dedup map and the parent→children
  // lookup map in #fetchBatched. If two values of different runtime
  // types collide on the same key string, the dedup conflates distinct
  // children into one bucket, dropping rows from the join output.

  test('number and string with same lexical form do not collide', () => {
    expect(canonicalKeyForTest({k: 1}, ['k'])).not.toBe(
      canonicalKeyForTest({k: '1'}, ['k']),
    );
  });

  test('number and bigint with same value do not collide', () => {
    // The actual production trigger: zqlite's safeIntegers returns
    // bigint for INTEGER columns, but a MemorySource on the other side
    // of the join would return number. Without the type tag, parents
    // (bigint) and children (number) on the same id would not match.
    expect(canonicalKeyForTest({k: 1}, ['k'])).not.toBe(
      canonicalKeyForTest({k: 1n}, ['k']),
    );
  });

  test('boolean and matching string do not collide', () => {
    // boolean true → "t", string "t" → "st"
    expect(canonicalKeyForTest({k: true}, ['k'])).not.toBe(
      canonicalKeyForTest({k: 't'}, ['k']),
    );
    expect(canonicalKeyForTest({k: false}, ['k'])).not.toBe(
      canonicalKeyForTest({k: 'f'}, ['k']),
    );
  });

  test('boolean and matching number do not collide', () => {
    // true → "t", 1 → "d1"; false → "f", 0 → "d0"
    expect(canonicalKeyForTest({k: true}, ['k'])).not.toBe(
      canonicalKeyForTest({k: 1}, ['k']),
    );
    expect(canonicalKeyForTest({k: false}, ['k'])).not.toBe(
      canonicalKeyForTest({k: 0}, ['k']),
    );
  });

  test('null and undefined are treated as the same key', () => {
    // Intentional — both map to "n". A SQL NULL and a missing column
    // value share the same dedup bucket.
    expect(canonicalKeyForTest({k: null}, ['k'])).toBe(
      canonicalKeyForTest({k: undefined}, ['k']),
    );
  });

  test('null and string "n" do not collide', () => {
    // null → "n", string "n" → "sn"
    expect(canonicalKeyForTest({k: null}, ['k'])).not.toBe(
      canonicalKeyForTest({k: 'n'}, ['k']),
    );
  });

  test('JSON value and string with same JSON repr do not collide', () => {
    // {a:1} → 'j{"a":1}', '{"a":1}' → 's{"a":1}'
    expect(canonicalKeyForTest({k: {a: 1}}, ['k'])).not.toBe(
      canonicalKeyForTest({k: '{"a":1}'}, ['k']),
    );
  });

  test('compound key: per-position type tagging', () => {
    // [1, "2"] vs ["1", 2] — same string concat without tags would
    // both look like "1<sep>2".
    expect(canonicalKeyForTest({a: 1, b: '2'}, ['a', 'b'])).not.toBe(
      canonicalKeyForTest({a: '1', b: 2}, ['a', 'b']),
    );
  });

  test('compound key: number vs bigint per position', () => {
    expect(canonicalKeyForTest({a: 1, b: 2}, ['a', 'b'])).not.toBe(
      canonicalKeyForTest({a: 1n, b: 2n}, ['a', 'b']),
    );
  });

  test('compound key: equal records produce equal keys', () => {
    // Sanity: matching tuples must be deduped together.
    expect(canonicalKeyForTest({a: 1, b: 'x'}, ['a', 'b'])).toBe(
      canonicalKeyForTest({a: 1, b: 'x'}, ['a', 'b']),
    );
    expect(canonicalKeyForTest({a: 1n, b: 'x'}, ['a', 'b'])).toBe(
      canonicalKeyForTest({a: 1n, b: 'x'}, ['a', 'b']),
    );
  });

  test('compound key: different positions are not interchangeable', () => {
    // Without the per-position separator, [1, 23] and [12, 3] could
    // collide when stringified naively.
    expect(canonicalKeyForTest({a: 1, b: 23}, ['a', 'b'])).not.toBe(
      canonicalKeyForTest({a: 12, b: 3}, ['a', 'b']),
    );
  });

  test('keys not present in the record collapse to null/undefined bucket', () => {
    // Reading a missing key returns undefined → tagged as "n" (same as
    // SQL NULL). Documents the existing behavior.
    expect(canonicalKeyForTest({}, ['k'])).toBe(
      canonicalKeyForTest({k: null}, ['k']),
    );
  });

  test('empty string does not collide with null/undefined', () => {
    // "" → "s" (just the type tag), null → "n". Without the tag they would
    // both render as the empty string.
    expect(canonicalKeyForTest({k: ''}, ['k'])).not.toBe(
      canonicalKeyForTest({k: null}, ['k']),
    );
    expect(canonicalKeyForTest({k: ''}, ['k'])).not.toBe(
      canonicalKeyForTest({k: undefined}, ['k']),
    );
  });

  test('falsy trio: 0n vs 0 vs false are all distinct', () => {
    // Common production overlap: a SQLite INTEGER column read with
    // safeIntegers gives bigint, a MemorySource gives number, and naive
    // user code might compare against `false`. All three must hash apart.
    const zeroBig = canonicalKeyForTest({k: 0n}, ['k']);
    const zeroNum = canonicalKeyForTest({k: 0}, ['k']);
    const falseBool = canonicalKeyForTest({k: false}, ['k']);
    expect(zeroBig).not.toBe(zeroNum);
    expect(zeroBig).not.toBe(falseBool);
    expect(zeroNum).not.toBe(falseBool);
  });
});

// Exercises the chunk-count branch boundary in `#fetchBatched`:
//   computedMulti.length <= multiConstraintChunkSize → single fetch
//   computedMulti.length >  multiConstraintChunkSize → #fetchChunked
// `cases` covers N = chunkSize (single-chunk, exact threshold) and
// N = 2*chunkSize (two full chunks, no remainder). The +1-remainder cases
// (5 with chunk=2 and `null FK` setup) are exercised by other tests.
test.each([
  {n: 2, chunkSize: 2, expectedChunks: [2]},
  {n: 4, chunkSize: 2, expectedChunks: [2, 2]},
])(
  'chunk boundary: $n children at chunk size $chunkSize → $expectedChunks',
  ({n, chunkSize, expectedChunks}) => {
    restoreChunkSize = setMultiConstraintChunkSizeForTest(chunkSize);

    const parent = createSource(
      lc,
      testLogConfig,
      'parent',
      {id: {type: 'string'}, label: {type: 'string'}},
      ['id'],
    );
    const child = createSource(
      lc,
      testLogConfig,
      'child',
      {id: {type: 'string'}, parentId: {type: 'string'}},
      ['id'],
    );

    for (let i = 1; i <= n; i++) {
      consume(
        parent.push(makeSourceChangeAdd({id: `p${i}`, label: `Parent ${i}`})),
      );
      consume(
        child.push(makeSourceChangeAdd({id: `c${i}`, parentId: `p${i}`})),
      );
    }

    const log: SnitchMessage[] = [];
    const parentSnitch = new Snitch(parent.connect([['id', 'asc']]), 'p', log);
    const childSnitch = new Snitch(child.connect([['id', 'asc']]), 'c', log);

    const fj = new FlippedJoin({
      parent: parentSnitch,
      child: childSnitch,
      parentKey: ['id'],
      childKey: ['parentId'],
      relationshipName: 'children',
      hidden: false,
      system: 'client',
    });

    const result = new Catch(fj).fetch({});
    expect(result.map(n => asRow(n).row.id)).toEqual(
      Array.from({length: n}, (_, i) => `p${i + 1}`),
    );

    const parentFetches = parentFetchMessages(log);
    expect(parentFetches).toHaveLength(expectedChunks.length);
    for (let i = 0; i < expectedChunks.length; i++) {
      expect(must(parentFetches[i][2].multiConstraints)[0]).toHaveLength(
        expectedChunks[i],
      );
    }
  },
);

test('children with null FK are silently dropped from the multi-constraint', () => {
  // buildJoinConstraint returns undefined when any source value is null,
  // and #fetchBatched skips those children when computing the dedup map.
  // Verify under chunking so we hit the multi-chunk path as well.
  restoreChunkSize = setMultiConstraintChunkSizeForTest(2);

  const parent = createSource(
    lc,
    testLogConfig,
    'parent',
    {id: {type: 'string'}, label: {type: 'string'}},
    ['id'],
  );
  const child = createSource(
    lc,
    testLogConfig,
    'child',
    {id: {type: 'string'}, parentId: {type: 'string'}},
    ['id'],
  );

  // 4 parents.
  for (let i = 1; i <= 4; i++) {
    consume(
      parent.push(makeSourceChangeAdd({id: `p${i}`, label: `Parent ${i}`})),
    );
  }
  // 5 children: c0 has null FK, c1..c4 each point at p1..p4. With chunk
  // size 2, the 4 valid children must split into 2 chunks (not 3), proving
  // the null-FK row was dropped before chunking.
  consume(child.push(makeSourceChangeAdd({id: 'c0', parentId: null})));
  for (let i = 1; i <= 4; i++) {
    consume(child.push(makeSourceChangeAdd({id: `c${i}`, parentId: `p${i}`})));
  }

  const log: SnitchMessage[] = [];
  const parentSnitch = new Snitch(parent.connect([['id', 'asc']]), 'p', log);
  const childSnitch = new Snitch(child.connect([['id', 'asc']]), 'c', log);

  const fj = new FlippedJoin({
    parent: parentSnitch,
    child: childSnitch,
    parentKey: ['id'],
    childKey: ['parentId'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });

  const result = new Catch(fj).fetch({});

  // Only the 4 parents with matching valid children appear; the null-FK
  // child is not reflected anywhere in the output.
  expect(result.map(n => asRow(n).row.id)).toEqual(['p1', 'p2', 'p3', 'p4']);

  // 2 chunks of 2 valid children (not 3) — the null-FK row was filtered
  // before chunking.
  const parentFetches = parentFetchMessages(log);
  expect(parentFetches).toHaveLength(2);
  expect(must(parentFetches[0][2].multiConstraints)[0]).toHaveLength(2);
  expect(must(parentFetches[1][2].multiConstraints)[0]).toHaveLength(2);
});

test('inprogress child REMOVE incompatible with req.constraint is dropped from multi', () => {
  // The `constraintsAreCompatible` skip in #fetchBatched (flipped-join.ts)
  // is a perf optimization for rows brought into childNodes by the
  // inprogressChildChange splice (the regular child fetch is already
  // filtered by the translated constraint, so its rows always match
  // req.constraint). The overlay-aware yield path makes the output
  // semantically correct either way, but without this filter the
  // source receives an extra IN entry whose value contradicts
  // req.constraint — wasted work and a misleading SQL shape.
  //
  // Setup: 3 parents/3 1:1 children. Push a REMOVE for c1 (parentId=p1)
  // and, during the resulting output.push, re-fetch with constraint
  // {id:'p2'}. The splice would put c1 back into childNodes, but its
  // parent-key {id:'p1'} conflicts with req.constraint and must be
  // dropped from the multi.
  const parent = createSource(
    lc,
    testLogConfig,
    'parent',
    {id: {type: 'string'}, label: {type: 'string'}},
    ['id'],
  );
  const child = createSource(
    lc,
    testLogConfig,
    'child',
    {id: {type: 'string'}, parentId: {type: 'string'}},
    ['id'],
  );

  for (let i = 1; i <= 3; i++) {
    consume(
      parent.push(makeSourceChangeAdd({id: `p${i}`, label: `Parent ${i}`})),
    );
    consume(child.push(makeSourceChangeAdd({id: `c${i}`, parentId: `p${i}`})));
  }

  const log: SnitchMessage[] = [];
  const parentSnitch = new Snitch(parent.connect([['id', 'asc']]), 'p', log);
  const childSnitch = new Snitch(child.connect([['id', 'asc']]), 'c', log);

  const fj = new FlippedJoin({
    parent: parentSnitch,
    child: childSnitch,
    parentKey: ['id'],
    childKey: ['parentId'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });

  let fetched: CaughtNode[] | undefined;
  fj.setOutput({
    push: () => {
      log.length = 0; // ignore the push-driven fetches; only care about ours
      fetched = new Catch(fj).fetch({constraint: {id: 'p2'}});
      return [];
    },
  });

  consume(child.push(makeSourceChangeRemove({id: 'c1', parentId: 'p1'})));

  // The output is still semantically correct (overlay drops p1).
  expect(fetched?.map(n => asRow(n).row.id)).toEqual(['p2']);

  // Critical assertion: the parent.fetch's multi must NOT contain
  // {id:'p1'} — the compatibility filter dropped the spliced c1.
  const parentFetches = parentFetchMessages(log);
  expect(parentFetches).toHaveLength(1);
  const multi = must(parentFetches[0][2].multiConstraints)[0];
  expect(multi).toEqual([{id: 'p2'}]);
});
