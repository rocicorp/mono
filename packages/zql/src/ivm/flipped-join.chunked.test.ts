/* oxlint-disable @typescript-eslint/no-explicit-any */
import {afterEach, expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Catch} from './catch.ts';
import {
  FlippedJoin,
  setMultiConstraintChunkSizeForTest,
} from './flipped-join.ts';
import {Snitch, type SnitchMessage} from './snitch.ts';
import {makeSourceChangeAdd} from './source.ts';
import {consume} from './stream.ts';
import {createSource} from './test/source-factory.ts';

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
  expect(result.map((n: any) => n.row.id)).toEqual([
    'p1',
    'p2',
    'p3',
    'p4',
    'p5',
  ]);
  for (const node of result) {
    const parentId = (node as any).row.id;
    const children = (node as any).relationships.children;
    expect(children).toHaveLength(1);
    expect(children[0].row.parentId).toBe(parentId);
  }

  // 1 child fetch, then 3 parent fetches (chunks of 2, 2, 1).
  const parentFetches = log.filter(msg => msg[0] === 'p' && msg[1] === 'fetch');
  expect(parentFetches).toHaveLength(3);
  // Each parent fetch should have been called with multiConstraint
  // matching the chunk-size cap.
  expect((parentFetches[0][2] as any).multiConstraint).toHaveLength(2);
  expect((parentFetches[1][2] as any).multiConstraint).toHaveLength(2);
  expect((parentFetches[2][2] as any).multiConstraint).toHaveLength(1);
});

test('single chunk path used when multiConstraint fits in one chunk', () => {
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
  expect(result.map((n: any) => n.row.id)).toEqual(['p1', 'p2', 'p3']);

  const parentFetches = log.filter(msg => msg[0] === 'p' && msg[1] === 'fetch');
  expect(parentFetches).toHaveLength(1);
  expect((parentFetches[0][2] as any).multiConstraint).toHaveLength(3);
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
  const wrappedParent: any = {
    ...parentInput,
    getSchema: () => parentInput.getSchema(),
    setOutput: (o: any) => parentInput.setOutput(o),
    destroy: () => parentInput.destroy(),
    fetch: (req: any) => {
      const idx = nextStreamIdx++;
      const inner = parentInput.fetch(req);
      return {
        [Symbol.iterator]() {
          const it = inner[Symbol.iterator]();
          return {
            next: () => it.next(),
            return(value?: any) {
              returnCalls.push(idx);
              return it.return?.(value) ?? {done: true, value: undefined};
            },
            [Symbol.iterator]() {
              return this;
            },
          };
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
    expect((node as any).relationships.children).toHaveLength(2);
  }

  // Multi-constraint should have 3 unique entries (one per parent), not 6.
  // With chunk size 2 that becomes 2 chunks (2 + 1), not 3.
  const parentFetches = log.filter(msg => msg[0] === 'p' && msg[1] === 'fetch');
  expect(parentFetches).toHaveLength(2);
  expect((parentFetches[0][2] as any).multiConstraint).toHaveLength(2);
  expect((parentFetches[1][2] as any).multiConstraint).toHaveLength(1);
});
