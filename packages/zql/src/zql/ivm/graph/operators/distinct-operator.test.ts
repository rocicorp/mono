import {expect, test, vi} from 'vitest';
import type {Entry, Multiset} from '../../multiset.js';
import {DifferenceStream, Listener} from '../difference-stream.js';

test('distinct', () => {
  type T = {
    id: string;
  };
  const input = new DifferenceStream<T>();
  const output = input.distinct();
  let version = 1;

  const items: Multiset<T>[] = [];
  output.debug((v, d) => {
    expect(v).toBe(version);
    items.push([...d]);
  });

  input.newDifference(
    version,
    [
      [{id: 'a'}, 1],
      [{id: 'b'}, 2],
      [{id: 'a'}, -1],
      [{id: 'c'}, -3],
    ],
    undefined,
  );
  input.commit(version);

  expect(items).toEqual([
    [
      [{id: 'a'}, 1],
      [{id: 'b'}, 1],
      [{id: 'a'}, -1],
      [{id: 'c'}, -1],
    ],
  ]);

  version++;
  items.length = 0;
  input.newDifference(version, [[{id: 'b'}, -2]], undefined);
  input.commit(version);
  expect(items).toEqual([[[{id: 'b'}, -1]]]);

  version++;
  items.length = 0;
  input.newDifference(version, [[{id: 'd'}, -1]], undefined);
  input.newDifference(version, [[{id: 'd'}, 1]], undefined);
  input.commit(version);
  expect(items).toEqual([[[{id: 'd'}, -1]], [[{id: 'd'}, 1]]]);

  version++;
  items.length = 0;
  input.newDifference(version, [[{id: 'e'}, -1]], undefined);
  input.newDifference(version, [[{id: 'e'}, 5]], undefined);
  input.commit(version);
  expect(items).toEqual([[[{id: 'e'}, -1]], [[{id: 'e'}, 2]]]);

  version++;
  items.length = 0;
  input.newDifference(version, [[{id: 'e'}, 5]], undefined);
  input.newDifference(version, [[{id: 'e'}, -6]], undefined);
  input.commit(version);
  expect(items).toEqual([[[{id: 'e'}, 1]], [[{id: 'e'}, -2]]]);
});

test('distinct all', () => {
  type T = {id: string};
  const input = new DifferenceStream<T>();
  const output = input.distinctAll(x => x.id);
  let version = 1;

  const items: Entry<T>[] = [];
  output.effect((item, mult) => {
    items.push([item, mult]);
  });

  input.newDifference(
    version,
    [
      [{id: 'a'}, 1],
      [{id: 'b'}, 2],
      [{id: 'a'}, -1],
      [{id: 'c'}, -3],
    ],
    undefined,
  );
  input.commit(version);
  check([
    [{id: 'a'}, 1],
    [{id: 'b'}, 1],
    [{id: 'a'}, -1],
    [{id: 'c'}, -1],
  ]);

  // output b and negative c
  input.newDifference(
    version,
    [
      [{id: 'b'}, 1],
      [{id: 'c'}, -1],
    ],
    undefined,
  );
  input.commit(version);
  check([]);

  // move c to positive
  input.newDifference(version, [[{id: 'c'}, 6]], undefined);
  input.commit(version);
  check([[{id: 'c'}, 1]]);

  // bring back a
  input.newDifference(version, [[{id: 'a'}, 1]], undefined);
  input.commit(version);
  check([[{id: 'a'}, 1]]);

  // delete b fully
  input.newDifference(version, [[{id: 'b'}, -3]], undefined);
  input.commit(version);
  check([[{id: 'b'}, -1]]);

  // more a and b. should be ignored.
  input.newDifference(
    version,
    [
      [{id: 'a'}, 1],
      [{id: 'b'}, -1],
    ],
    undefined,
  );
  input.commit(version);
  check([]);

  function check(expected: Entry<T>[]) {
    expect(items).toEqual(expected);
    items.length = 0;
    ++version;
  }
});

test('lazy', () => {
  type T = {id: number};
  const input = new DifferenceStream<T>();
  const output = input.distinct();
  const items: Multiset<T>[] = [];
  output.debug((_, d) => {
    items.push(d);
  });

  let called = 0;
  const infinite = {
    *[Symbol.iterator]() {
      for (let i = 0; ; i++) {
        ++called;
        yield [{id: i}, 1] as const;
      }
    },
  };

  input.newDifference(1, infinite, undefined);
  input.commit(1);

  // we run the graph but the mapper is not run until we pull on it
  expect(called).toBe(0);

  // drain some items
  const generator = items[0];
  for (const x of generator) {
    if (x[0].id === 9) {
      break;
    }
  }
  expect(called).toBe(10);
});

test('re-pulling the same iterable more than once yields the same data', () => {
  type T = {id: number};
  const input = new DifferenceStream<T>();
  const output = input.distinct();
  const items: Multiset<T>[] = [];
  output.debug((_, d) => {
    items.push(d);
  });

  const data = [
    [{id: 1}, 1],
    [{id: 2}, 1],
    [{id: 1}, -1],
    [{id: 3}, 1],
  ] as const;

  input.newDifference(1, data, undefined);
  input.commit(1);

  const generator = items[0];
  const first = [...generator];
  const second = [...generator];
  expect(first).toEqual(second);
});

test('messageUpstream', () => {
  // Given the following graph:
  //
  //    A
  //    |
  // Distinct
  //    |
  //    B
  //
  // test that multiple upstream messages with the same id will only call
  // newDifference once.

  type T = {id: string; value: number};
  const a = new DifferenceStream<T>();
  const newDifferenceSpy = vi.fn<Listener<T>['newDifference']>();
  const b = a.distinct();
  const listener: Listener<T> = {
    commit() {},
    newDifference: newDifferenceSpy,
  };
  const requestedID = 123;
  b.messageUpstream(
    {id: requestedID, type: 'pull', hoistedConditions: []},
    listener,
  );
  b.messageUpstream(
    {id: requestedID, type: 'pull', hoistedConditions: []},
    listener,
  );

  expect(newDifferenceSpy).toHaveBeenCalledTimes(0);

  const version = 1;
  a.newDifference(version, [[{id: 'a', value: 1}, 1]], {
    replyingTo: requestedID,
    sourceName: 'A',
    order: undefined,
    type: 'pullResponse',
    contiguousGroup: [],
  });
  a.commit(version);

  expect(newDifferenceSpy).toHaveBeenCalledTimes(1);
  expect(newDifferenceSpy).toHaveBeenCalledWith(
    version,
    expect.objectContaining({[Symbol.iterator]: expect.any(Function)}),
    {
      replyingTo: requestedID,
      sourceName: 'A',
      order: undefined,
      type: 'pullResponse',
      contiguousGroup: [],
    },
  );
});
