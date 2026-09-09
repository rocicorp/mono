/**
 * Regressions from the `Stream` -> `PullStream` conversion.
 *
 * A generator got cleanup for free: `for...of` called `.return()` on abrupt
 * completion, and a `finally` in the generator body ran on close. The pull
 * protocol makes both explicit, and these are the places the explicit version
 * was not written. Each test is named for the finding it reproduces.
 */
import {describe, expect, test, vi} from 'vitest';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {Cap} from './cap.ts';
import {makeAddChange} from './change.ts';
import type {Node} from './data.ts';
import {DeferredInput} from './deferred-input.ts';
import {Exists} from './exists.ts';
import {FanOut} from './fan-out.ts';
import {
  FilterStart,
  type FilterInput,
  type FilterOutput,
} from './filter-operators.ts';
import {mergeSortedStreams} from './memory-source.ts';
import {MemoryStorage} from './memory-storage.ts';
import type {FetchRequest, Input, Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {consume, drainPull, pullOf, type PullStream} from './stream.ts';
import {Take} from './take.ts';
import {mergeFetches} from './union-fan-in.ts';
import {applyChange, type ViewChange} from './view-apply-change.ts';
import type {Entry, Format} from './view.ts';

/** A stream that records whether the consumer released it. */
type TrackedStream<T> = PullStream<T> & {readonly closed: boolean};

function trackedPull<T>(values: readonly T[]): TrackedStream<T> {
  let i = 0;
  let closed = false;
  return {
    next: () => (i < values.length ? values[i++] : undefined),
    close() {
      closed = true;
    },
    get closed() {
      return closed;
    },
  };
}

/** Fails on the `n`th pull, after handing back `before`. */
function throwingPull<T>(
  error: Error,
  before: readonly T[] = [],
): TrackedStream<T> {
  let i = 0;
  let closed = false;
  return {
    next() {
      if (i < before.length) {
        return before[i++];
      }
      throw error;
    },
    close() {
      closed = true;
    },
    get closed() {
      return closed;
    },
  };
}

const node = (id: number): Node => ({row: {id}, relationships: {}});

function schema(
  relationships: Record<string, SourceSchema> = {},
): SourceSchema {
  return {
    tableName: 'test',
    columns: {id: {type: 'number'}},
    primaryKey: ['id'],
    relationships,
    isHidden: false,
    system: 'client',
    compareRows: (a, b) => (a.id as number) - (b.id as number),
    sort: [['id', 'asc']],
  };
}

function filterOutput(
  verdict: (node: Node) => boolean | 'yield',
): FilterOutput {
  return {
    push: vi.fn(),
    beginFilter: vi.fn(),
    endFilter: vi.fn(),
    filter: vi.fn(verdict),
  };
}

function stubInput(fetch: () => PullStream<Node | 'yield'>): Input {
  return {
    setOutput: vi.fn(),
    fetch,
    destroy: vi.fn(),
    getSchema: () => schema(),
  };
}

function stubFilterInput(s: SourceSchema = schema()): FilterInput {
  return {
    setFilterOutput: vi.fn(),
    destroy: vi.fn(),
    getSchema: () => s,
  };
}

describe('#2 FilterStart leaks its input on a throw', () => {
  test('closes the input stream when a downstream filter throws', () => {
    const rows = trackedPull<Node | 'yield'>([node(1), node(2)]);
    const output = filterOutput(() => {
      throw new Error('predicate blew up');
    });

    const start = new FilterStart(stubInput(() => rows));
    start.setFilterOutput(output);

    const stream = start.fetch({} as FetchRequest);
    try {
      expect(() => stream.next()).toThrow('predicate blew up');
    } finally {
      // What every converted call site does. It is a no-op here because
      // next()'s catch already set #ended, so #input.close() is never reached.
      stream.close();
    }

    expect(output.endFilter).toHaveBeenCalledTimes(1);
    expect(rows.closed).toBe(true);
  });
});

describe('#3 Exists drops its in-flight count stream', () => {
  const makeExists = () => {
    const exists = new Exists(
      stubFilterInput(schema({rel: schema()})),
      'rel',
      ['id'],
      'EXISTS',
    );
    exists.setFilterOutput(filterOutput(() => true));
    return exists;
  };

  test('releases a suspended count stream when the scan ends', () => {
    const exists = makeExists();
    const children = trackedPull<Node | 'yield'>(['yield', node(10)]);
    const parent: Node = {row: {id: 1}, relationships: {rel: () => children}};

    exists.beginFilter();
    expect(exists.filter(parent)).toBe('yield');

    // The scan is abandoned while Exists is suspended mid-count. Closing the
    // fetch stream reaches Exists only through endFilter() -- Exists is a
    // FilterOutput, not a link in the stream chain -- so this is the one
    // callback that can release the child stream.
    exists.endFilter();

    expect(children.closed).toBe(true);
  });

  test('releases a suspended count stream when a different node arrives', () => {
    const exists = makeExists();
    const first = trackedPull<Node | 'yield'>(['yield', node(10)]);
    const second = trackedPull<Node | 'yield'>([node(20)]);

    exists.beginFilter();
    expect(
      exists.filter({row: {id: 1}, relationships: {rel: () => first}}),
    ).toBe('yield');
    // #pending is overwritten wholesale; the half-read `first` is dropped.
    expect(
      exists.filter({row: {id: 2}, relationships: {rel: () => second}}),
    ).toBe(true);

    expect(first.closed).toBe(true);
  });
});

describe('#4 FanOut carries #filterIndex across scans', () => {
  test('re-evaluates from the first branch after an abandoned scan', () => {
    const fanOut = new FanOut(stubFilterInput());
    const branchA = filterOutput(n => n.row.id === 2);
    const branchB = filterOutput(n => (n.row.id === 1 ? 'yield' : false));
    fanOut.setFilterOutput(branchA);
    fanOut.setFilterOutput(branchB);

    fanOut.beginFilter();
    // Branch A rejects node 1; branch B suspends on it.
    expect(fanOut.filter(node(1))).toBe('yield');

    // The scan is abandoned without resuming, and a new one begins.
    fanOut.endFilter();
    fanOut.beginFilter();

    // Node 2 matches branch A only. #filterIndex is still 1, so A is skipped
    // and a row that belongs in the OR result is dropped.
    expect(fanOut.filter(node(2))).toBe(true);
    expect(branchA.filter).toHaveBeenCalledWith(node(2));
  });
});

describe('#5 merges leak sibling streams on a throw', () => {
  const byId = (l: Node, r: Node) =>
    (l.row.id as number) - (r.row.id as number);

  test('mergeFetches closes the other branches', () => {
    const a = trackedPull<Node | 'yield'>([node(1)]);
    const b = throwingPull<Node | 'yield'>(new Error('branch exploded'));
    const c = trackedPull<Node | 'yield'>([node(3)]);

    const merged = mergeFetches([a, b, c], byId);
    expect(() => drainPull(merged)).toThrow('branch exploded');

    expect(a.closed).toBe(true);
    expect(c.closed).toBe(true);
  });

  test('mergeSortedStreams closes the other chunks', () => {
    const a = trackedPull<Node | 'yield'>([node(1)]);
    const b = throwingPull<Node | 'yield'>(new Error('chunk exploded'));
    const c = trackedPull<Node | 'yield'>([node(3)]);

    const merged = mergeSortedStreams([a, b, c], byId);
    expect(() => drainPull(merged)).toThrow('chunk exploded');

    expect(a.closed).toBe(true);
    expect(c.closed).toBe(true);
  });
});

describe('#6 consumer loops never close their stream', () => {
  test('applyChange releases the child stream when the body throws', () => {
    const children = trackedPull<Node | 'yield'>([node(1), node(2)]);
    const format: Format = {
      singular: false,
      relationships: {rel: {singular: true, relationships: {}}},
    };
    const change: ViewChange = {
      type: 'add',
      node: {row: {id: 100}, relationships: {rel: () => children}},
    };

    // Two distinct rows for a `singular` relationship: the assert fires from
    // inside the loop body, exactly the abrupt completion `for...of` covered.
    expect(() =>
      applyChange(
        {'': []} as Entry,
        change,
        schema({rel: schema()}),
        '',
        format,
        false,
        false,
      ),
    ).toThrow(/should not have multiple rows/);

    expect(children.closed).toBe(true);
  });

  test('DeferredInput.attach releases its fetch stream when hydration throws', () => {
    const rows = trackedPull<Node | 'yield'>([node(1), node(2)]);
    const deferred = new DeferredInput(schema(), () => stubInput(() => rows));
    deferred.setOutput({
      push: () => {
        throw new Error('view rejected the row');
      },
    } as unknown as Output);

    expect(() => deferred.attach()).toThrow('view rejected the row');
    expect(rows.closed).toBe(true);
  });
});

describe('#8 Cap is eager where Take is lazy', () => {
  test('defers the input fetch to the first next()', () => {
    let capFetches = 0;
    const cap = new Cap(
      stubInput(() => {
        capFetches++;
        return pullOf([node(1)]);
      }),
      new MemoryStorage(),
      10,
    );

    const capStream = cap.fetch({});
    expect(capFetches).toBe(0);
    capStream.next();
    expect(capFetches).toBe(1);
  });

  test('Take, converted in the same change, does defer', () => {
    let takeFetches = 0;
    const take = new Take(
      stubInput(() => {
        takeFetches++;
        return pullOf([node(1)]);
      }),
      new MemoryStorage(),
      10,
    );

    const takeStream = take.fetch({});
    expect(takeFetches).toBe(0);
    takeStream.next();
    expect(takeFetches).toBe(1);
  });
});

describe('#9 Take snapshots #rowHiddenFromFetch', () => {
  // Ordered rows, honouring the `start`/`reverse` fetches Take's push path
  // makes. A stub rather than a MemorySource on purpose: a source-backed fetch
  // stream consumed across a push boundary emits the pushed row twice (its
  // overlay, then the committed row), so it cannot isolate Take's own
  // behaviour. See the note in the report.
  const sorted: SourceSchema = {
    tableName: 'issue',
    columns: {id: {type: 'string'}, created: {type: 'number'}},
    primaryKey: ['id'],
    relationships: {},
    isHidden: false,
    system: 'client',
    compareRows: (a, b) =>
      (a.created as number) - (b.created as number) ||
      (a.id as string).localeCompare(b.id as string),
    sort: [
      ['created', 'asc'],
      ['id', 'asc'],
    ],
  };

  function sortedInput(rows: Row[]): Input {
    return {
      setOutput: vi.fn(),
      destroy: vi.fn(),
      getSchema: () => sorted,
      fetch(req: FetchRequest) {
        const cmp = req.reverse
          ? (a: Row, b: Row) => sorted.compareRows(b, a)
          : sorted.compareRows;
        let out = rows.toSorted(cmp);
        const {start} = req;
        if (start) {
          const i = out.findIndex(r => cmp(r, start.row) >= 0);
          const from = i === -1 ? out.length : start.basis === 'at' ? i : i + 1;
          out = out.slice(from);
        }
        return pullOf(out.map(row => ({row, relationships: {}})));
      },
    };
  }

  test('a scan started during a push stops hiding once the push ends', () => {
    const rows: Row[] = [
      {id: 'i1', created: 100},
      {id: 'i2', created: 200},
      {id: 'i3', created: 300},
    ];
    const take = new Take(sortedInput(rows), new MemoryStorage(), 2);

    const ids = (nodes: (Node | 'yield')[]) =>
      nodes.filter(n => n !== 'yield').map(n => (n as Node).row.id);
    expect(ids(drainPull(take.fetch({})))).toEqual(['i1', 'i2']);

    // Capture a fetch stream mid-push, while `i15` is hidden, and pull one
    // node from it so the scan is genuinely underway and `hidden` is bound.
    let captured: PullStream<Node | 'yield'> | undefined;
    let duringPush: (Node | 'yield')[] = [];
    take.setOutput({
      *push() {
        if (captured === undefined) {
          captured = take.fetch({});
          const first = captured.next();
          if (first !== undefined) {
            duringPush = [first];
          }
        }
      },
    } as unknown as Output);

    const added = {id: 'i15', created: 150};
    rows.push(added);
    consume(take.push(makeAddChange({row: added, relationships: {}})));

    // While the push was in flight, i15 was correctly hidden.
    expect(ids(duringPush)).toEqual(['i1']);

    // The push's `finally` has now cleared #rowHiddenFromFetch, so the rest of
    // the scan should see i15. The hoisted `const hidden` keeps hiding it.
    expect(ids(drainPull(captured!))).toEqual(['i15']);
  });
});
