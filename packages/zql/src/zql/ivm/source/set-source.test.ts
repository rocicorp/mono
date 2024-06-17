import fc from 'fast-check';
import {describe, expect, test} from 'vitest';
import type {Ordering} from '../../ast/ast.js';
import type {Listener} from '../graph/difference-stream.js';
import type {PullMsg} from '../graph/message.js';
import {Materialite} from '../materialite.js';
import {mergeRequests} from './set-source.js';

type E = {id: number};

const ordering: Ordering = [[['test', 'id'], 'asc']];
const descOrdering: Ordering = [[['test', 'id'], 'desc']];
const comparator = (l: E, r: E) => l.id - r.id;
const numberComparator = (l: number, r: number) => l - r;

test('add', () => {
  fc.assert(
    fc.property(fc.uniqueArray(fc.integer()), arr => {
      const m = new Materialite();
      const source = m.newSetSource(comparator, ordering, 'test');

      arr.forEach(x => source.add({id: x}));
      expect([...source.value.keys()]).toEqual(
        arr.sort(numberComparator).map(x => ({id: x})),
      );
    }),
  );
});

test('delete', () => {
  fc.assert(
    fc.property(fc.uniqueArray(fc.integer()), arr => {
      const m = new Materialite();
      const source = m.newSetSource(comparator, ordering, 'test');

      arr.forEach(x => source.add({id: x}));
      arr.forEach(x => source.delete({id: x}));
      expect([...source.value.keys()]).toEqual([]);
    }),
  );
});

test('on', () => {
  const m = new Materialite();
  const source = m.newSetSource(comparator, ordering, 'test');

  let callCount = 0;
  const dispose = source.on(value => {
    expect(value).toEqual(source.value);
    ++callCount;

    expect([...value.keys()]).toEqual([{id: 1}, {id: 2}]);
  });
  m.tx(() => {
    source.add({id: 1});
    source.add({id: 2});
    source.delete({id: 3});
  });

  // only called at the end of a transaction.
  expect(callCount).toBe(1);

  dispose();

  m.tx(() => {
    source.add({id: 3});
  });

  // not notified if the listener is removed
  expect(callCount).toBe(1);

  // TODO: don't notify if the value didn't change?
  // We could track this in the source by checking if add events returned false
});

test('replace', () => {
  fc.assert(
    fc.property(fc.uniqueArray(fc.integer()), arr => {
      const m = new Materialite();
      const source = m.newSetSource(comparator, ordering, 'test');

      m.tx(() => {
        arr.forEach(id => source.add({id}));
      });

      m.tx(() => {
        arr.forEach(id => {
          // We have special affordances for deletes immediately followed by adds
          // As those are really replaces.
          // Check that the source handles this correctly.
          source.delete({id});
          source.add({id});
        });
      });

      expect([...source.value.keys()]).toEqual(
        arr.map(id => ({id})).sort(comparator),
      );
    }),
  );
});

// we don't do any rollbacks. If Materialite throws then
// it has diverged from Replicache and we're in a bad state.
test('rollback', async () => {
  const m = new Materialite();
  const source = m.newSetSource(comparator, ordering, 'test');

  try {
    m.tx(() => {
      source.add({id: 1});
      throw new Error('rollback');
    });
  } catch (e) {
    // ignore
  }
  await Promise.resolve();

  expect([...source.value.keys()]).toEqual([]);

  source.add({id: 2});
  await Promise.resolve();
  expect([...source.value.keys()]).toEqual([{id: 2}]);
});

test('withNewOrdering - we do not update the derived thing / withNewOrdering is not tied to the original. User must do that.', async () => {
  const m = new Materialite();
  const source = m.newSetSource(comparator, ordering, 'test');
  const derived = source.withNewOrdering((l, r) => r.id - l.id, descOrdering);

  m.tx(() => {
    source.add({id: 1});
    source.add({id: 2});
  });
  await Promise.resolve();

  expect([...source.value.keys()]).toEqual([{id: 1}, {id: 2}]);
  expect([...derived.value.keys()]).toEqual([]);
});

test('withNewOrdering - is correctly ordered', async () => {
  const m = new Materialite();

  await fc.assert(
    fc.asyncProperty(fc.uniqueArray(fc.integer()), async arr => {
      const source = m.newSetSource(comparator, ordering, 'test');
      const derived = source.withNewOrdering(
        (l, r) => r.id - l.id,
        descOrdering,
      );
      m.tx(() => {
        arr.forEach(id => {
          source.add({id});
          derived.add({id});
        });
      });
      await Promise.resolve();

      expect([...source.value.keys()]).toEqual(
        arr.map(id => ({id})).sort(comparator),
      );
      expect([...derived.value.keys()]).toEqual(
        arr.sort((l, r) => r - l).map(id => ({id})),
      );
    }),
  );
});

test('history requests with an alternate ordering are fulfilled by that ordering', () => {
  type E2 = {
    id: number;
    x: string;
  };
  const comparator = (l: E2, r: E2) => l.id - r.id;

  const ordering: Ordering = [[['e2', 'id'], 'asc']];
  const m = new Materialite();
  const source = m.newSetSource(comparator, ordering, 'e2');

  const baseItems = [
    {id: 1, x: 'c'},
    {id: 2, x: 'b'},
    {id: 3, x: 'a'},
  ];
  m.tx(() => {
    source.seed(baseItems);
  });

  const items: E2[] = [];
  const listener: Listener<E2> = {
    commit(_version) {},
    newDifference(_version, multiset, _reply) {
      for (const item of multiset) {
        items.push(item[0]);
      }
    },
  };
  m.tx(() => {
    source.stream.messageUpstream(
      {
        id: 1,
        type: 'pull',
        order: [[['e2', 'id'], 'asc']],
        hoistedConditions: [],
      },
      listener,
    );
  });

  expect(items).toEqual(baseItems);
  items.length = 0;

  m.tx(() => {
    source.stream.messageUpstream(
      {
        id: 2,
        type: 'pull',
        order: [
          [['e2', 'x'], 'asc'],
          [['e2', 'id'], 'asc'],
        ],
        hoistedConditions: [],
      },
      listener,
    );
  });

  expect(items).toEqual(
    baseItems.slice().sort((l, r) => l.x.localeCompare(r.x)),
  );
  items.length = 0;

  // add some data to see that we're maintained past seed phase
  source.add({id: 4, x: 'd'});

  m.tx(() => {
    source.stream.messageUpstream(
      {
        id: 3,
        type: 'pull',
        order: [
          [['e2', 'x'], 'asc'],
          [['e2', 'id'], 'asc'],
        ],
        hoistedConditions: [],
      },
      listener,
    );
  });

  expect(items).toEqual(
    baseItems.concat({id: 4, x: 'd'}).sort((l, r) => l.x.localeCompare(r.x)),
  );

  items.length = 0;

  m.tx(() => {
    source.stream.messageUpstream(
      {
        id: 3,
        type: 'pull',
        order: [
          [['e2', 'x'], 'desc'],
          [['e2', 'id'], 'desc'],
        ],
        hoistedConditions: [],
      },
      listener,
    );
  });

  expect(items).toEqual(
    baseItems.concat({id: 4, x: 'd'}).sort((l, r) => -l.x.localeCompare(r.x)),
  );
});

describe('history requests with hoisted filters', () => {
  type E = {id: number; x: string; y: string};
  const baseItems: readonly E[] = [
    {id: 1, x: 'a', y: 'q'},
    {id: 2, x: 'b', y: 'q'},
    {id: 3, x: 'c', y: 'q'},
    {id: 4, x: 'd', y: 'r'},
    {id: 5, x: 'e', y: 'r'},
  ];

  test.each([
    {
      name: 'pk lookup',
      requests: [1, 2, 3].map(id => ({
        id,
        type: 'pull',
        order: undefined,
        hoistedConditions: [
          {
            selector: ['e', 'id'],
            op: '=',
            value: id,
          },
        ],
      })),
      expected: baseItems.map(item => [item]),
    },
    {
      name: 'pk lookup w/ order-by',
      requests: [1, 2, 3].map(id => ({
        id,
        type: 'pull',
        order: [
          [['e', 'x'], 'asc'],
          [['e', 'id'], 'asc'],
        ],
        hoistedConditions: [
          {
            selector: ['e', 'id'],
            op: '=',
            value: id,
          },
        ],
      })),
      expected: baseItems.map(item => [item]),
    },
    {
      name: 'pk asc w/ lower bound',
      requests: [
        {
          id: 1,
          type: 'pull',
          order: undefined,
          hoistedConditions: [
            {
              selector: ['e', 'id'],
              // yes, it is `>` but we treat it the same as `>=` in the source
              // since downstream filters will remove the off by one.
              op: '>',
              value: 2,
            },
          ],
        },
      ],
      expected: [[baseItems[1], baseItems[2], baseItems[3], baseItems[4]]],
    },
    {
      name: 'pk w/ lower and upper bound. asc.',
      requests: [
        {
          id: 1,
          type: 'pull',
          order: undefined,
          hoistedConditions: [
            {
              selector: ['e', 'id'],
              op: '>',
              value: 2,
            },
            {
              selector: ['e', 'id'],
              op: '<',
              value: 3,
            },
          ],
        },
      ],
      expected: [[baseItems[1], baseItems[2]]],
    },
    {
      name: 'pk w/ upper bound. asc.',
      requests: [
        {
          id: 1,
          type: 'pull',
          order: undefined,
          hoistedConditions: [
            {
              selector: ['e', 'id'],
              // yes, it is `<` but we treat it the same as `<=` in the source
              // since downstream filters will remove the off by one.
              op: '<',
              value: 3,
            },
          ],
        },
      ],
      expected: [[baseItems[0], baseItems[1], baseItems[2]]],
    },
    {
      name: 'just a naked request',
      requests: [
        {
          id: 1,
          type: 'pull',
          order: undefined,
          hoistedConditions: [],
        },
      ],
      expected: [baseItems],
    },
    {
      name: 'pk w/ lower bound. desc.',
      requests: [
        {
          id: 1,
          type: 'pull',
          order: [[['e', 'id'], 'desc']],
          hoistedConditions: [
            {
              selector: ['e', 'id'],
              op: '>',
              value: 3,
            },
          ],
        },
      ],
      expected: [[baseItems[4], baseItems[3], baseItems[2]]],
    },
    {
      name: 'pk w/ upper bound. desc.',
      requests: [
        {
          id: 1,
          type: 'pull',
          order: [[['e', 'id'], 'desc']],
          hoistedConditions: [
            {
              selector: ['e', 'id'],
              op: '<',
              value: 3,
            },
          ],
        },
      ],
      expected: [[baseItems[2], baseItems[1], baseItems[0]]],
    },
    {
      name: 'pk w/ lower and upper bound. desc.',
      requests: [
        {
          id: 1,
          type: 'pull',
          order: [[['e', 'id'], 'desc']],
          hoistedConditions: [
            {
              selector: ['e', 'id'],
              op: '>',
              value: 2,
            },
            {
              selector: ['e', 'id'],
              op: '<',
              value: 4,
            },
          ],
        },
      ],
      expected: [[baseItems[3], baseItems[2], baseItems[1]]],
    },
    {
      name: 'x lookup w/ order-by x',
      requests: ['a', 'b', 'c'].map(x => ({
        id: 1,
        type: 'pull',
        order: [
          [['e', 'x'], 'asc'],
          [['e', 'id'], 'asc'],
        ],
        hoistedConditions: [
          {
            selector: ['e', 'x'],
            op: '=',
            value: x,
          },
        ],
      })),
      // we constrained on `x` so only 1 item is processed.
      expected: [[baseItems[0]], [baseItems[1]], [baseItems[2]]],
    },
    {
      name: 'x lookup w/ order-by y',
      requests: ['a', 'b', 'c'].map(x => ({
        id: 1,
        type: 'pull',
        order: [
          [['e', 'y'], 'asc'],
          [['e', 'id'], 'asc'],
        ],
        hoistedConditions: [
          {
            selector: ['e', 'x'],
            op: '=',
            value: x,
          },
        ],
      })),
      // we can't constrain on `x` so all baseItems are processed.
      expected: [baseItems, baseItems, baseItems],
    },
    {
      name: 'x asc w/ lower bound on x',
      requests: [
        {
          id: 1,
          type: 'pull',
          order: [
            [['e', 'x'], 'asc'],
            [['e', 'id'], 'asc'],
          ],
          hoistedConditions: [
            {
              selector: ['e', 'x'],
              op: '>',
              value: 'c',
            },
          ],
        },
      ],
      // we only process items >= c. Yes, >=. Downstream filters will remove the off by one.
      expected: [[baseItems[2], baseItems[3], baseItems[4]]],
    },
    {
      name: 'lookup on y w/ order-by y',
      requests: [
        {
          id: 1,
          type: 'pull',
          order: [
            [['e', 'y'], 'asc'],
            [['e', 'id'], 'asc'],
          ],
          hoistedConditions: [
            {
              selector: ['e', 'y'],
              op: '=',
              value: 'q',
            },
          ],
        },
      ],
      // only y values with `q` will be processed.
      expected: [baseItems.slice(0, 3)],
    },
    {
      name: 'lookup on x w/ order by x asc, id asc',
      requests: [
        {
          id: 1,
          type: 'pull',
          order: [
            [['e', 'x'], 'asc'],
            [['e', 'id'], 'asc'],
          ],
          hoistedConditions: [
            {
              selector: ['e', 'x'],
              op: '>=',
              value: 'b',
            },
          ],
        },
      ],
      seedItems: [
        {id: 1, x: 'a', y: 'q'},
        {id: 2, x: 'a', y: 'q'},
        {id: 3, x: 'b', y: 'q'},
        {id: 4, x: 'b', y: 'r'},
        {id: 5, x: 'c', y: 'r'},
        {id: 6, x: 'c', y: 'r'},
      ],
      expected: [
        [
          {id: 3, x: 'b', y: 'q'},
          {id: 4, x: 'b', y: 'r'},
          {id: 5, x: 'c', y: 'r'},
          {id: 6, x: 'c', y: 'r'},
        ],
      ],
    },

    {
      name: 'lookup on x w/ order by x desc, id asc',
      requests: [
        {
          id: 1,
          type: 'pull',
          order: [
            [['e', 'x'], 'desc'],
            [['e', 'id'], 'asc'],
          ],
          hoistedConditions: [
            {
              selector: ['e', 'x'],
              op: '>=',
              value: 'b',
            },
          ],
        },
      ],
      seedItems: [
        {id: 1, x: 'a', y: 'q'},
        {id: 2, x: 'a', y: 'q'},
        {id: 3, x: 'b', y: 'q'},
        {id: 4, x: 'b', y: 'r'},
        {id: 5, x: 'c', y: 'r'},
        {id: 6, x: 'c', y: 'r'},
      ],
      expected: [
        [
          // The result is ordered x desc, id desc
          {id: 6, x: 'c', y: 'r'},
          {id: 5, x: 'c', y: 'r'},
          {id: 4, x: 'b', y: 'r'},
          {id: 3, x: 'b', y: 'q'},
        ],
      ],
    },

    {
      name: 'lookup on x w/ order by x asc, id desc',
      requests: [
        {
          id: 1,
          type: 'pull',
          order: [
            [['e', 'x'], 'asc'],
            [['e', 'id'], 'desc'],
          ],
          hoistedConditions: [
            {
              selector: ['e', 'x'],
              op: '>=',
              value: 'b',
            },
          ],
        },
      ],
      seedItems: [
        {id: 1, x: 'a', y: 'q'},
        {id: 2, x: 'a', y: 'q'},
        {id: 3, x: 'b', y: 'q'},
        {id: 4, x: 'b', y: 'r'},
        {id: 5, x: 'c', y: 'r'},
        {id: 6, x: 'c', y: 'r'},
      ],
      expected: [
        [
          // The result is ordered x asc, id asc
          {id: 3, x: 'b', y: 'q'},
          {id: 4, x: 'b', y: 'r'},
          {id: 5, x: 'c', y: 'r'},
          {id: 6, x: 'c', y: 'r'},
        ],
      ],
    },
  ] satisfies {
    name: string;
    requests: PullMsg[];
    expected: readonly (readonly E[])[];
    seedItems?: readonly E[] | undefined;
  }[])(
    '$name',
    ({
      requests,
      expected,
      seedItems = baseItems,
    }: {
      name: string;
      requests: PullMsg[];
      expected: readonly (readonly E[])[];
      seedItems?: readonly E[] | undefined;
    }) => {
      const ordering: Ordering = [[['e', 'id'], 'asc']];
      const m = new Materialite();
      const comparator = (l: E, r: E) => l.id - r.id;
      const source = m.newSetSource(comparator, ordering, 'e');
      m.tx(() => {
        source.seed(seedItems);
      });

      const items: E[] = [];
      const listener: Listener<E> = {
        commit(_version) {},
        newDifference(_version, multiset, _reply) {
          for (const item of multiset) {
            items.push(item[0]);
          }
        },
      };
      requests.forEach((request, i) => {
        m.tx(() => {
          source.stream.messageUpstream(request, listener);
        });
        expect(items).toEqual(expected[i]);
        items.length = 0;
      });
    },
  );
});

describe('merge requests', () => {
  test.each([
    {
      name: 'no conditions or order',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [],
      },
    },
    {
      name: 'disjoint values',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 2,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [],
      },
    },
    {
      name: 'disjoint selectors',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'boo'],
            op: '=',
            value: 1,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [],
      },
    },
    {
      name: 'disjoint columns',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'baz'],
            op: '>',
            value: 1,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [],
      },
    },
    {
      name: 'widen >,=',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 1,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>=',
            value: 1,
          },
        ],
      },
    },
    {
      name: 'widen >,= 2',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 0,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>=',
            value: 0,
          },
        ],
      },
    },
    {
      name: 'widen >,= 3',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 4,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 1,
          },
        ],
      },
    },
    {
      name: 'widen >,>',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 0,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 0,
          },
        ],
      },
    },
    {
      name: 'widen >,> 2',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 1,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 1,
          },
        ],
      },
    },
    {
      name: 'widen >,> 3',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 2,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 1,
          },
        ],
      },
    },
    {
      name: 'widen >,=',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 1,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>=',
            value: 1,
          },
        ],
      },
    },
    {
      name: 'widen >,<',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '<',
            value: 2,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [],
      },
    },
    {
      name: 'widen =,>',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 0,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 0,
          },
        ],
      },
    },
    {
      name: 'widen =,<',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '<',
            value: 2,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '<',
            value: 2,
          },
        ],
      },
    },
    {
      name: 'widen <,>',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '<',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '>',
            value: 0,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [],
      },
    },
    {
      name: 'widen <,=',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '<',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 0,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '<',
            value: 1,
          },
        ],
      },
    },
    {
      name: 'widen <,= 2',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '<',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 2,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '<=',
            value: 2,
          },
        ],
      },
    },
    {
      name: 'widen <,<',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '<',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '<',
            value: 0,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '<',
            value: 1,
          },
        ],
      },
    },
    {
      name: 'widen <,<=',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '<',
            value: 1,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '<=',
            value: 2,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '<=',
            value: 2,
          },
        ],
      },
    },
    {
      name: 'partial overlap',
      reqA: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 1,
          },
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 2,
          },
        ],
      },
      reqB: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 1,
          },
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 3,
          },
        ],
      },
      expected: {
        id: 1,
        type: 'pull',
        hoistedConditions: [
          {
            selector: ['foo', 'bar'],
            op: '=',
            value: 1,
          },
        ],
      },
    },
    {
      name: 'previous issue page - zeppliear',
      reqA: {
        id: 5,
        type: 'pull',
        hoistedConditions: [
          {selector: ['issue', 'modified'], op: '>', value: 5004533700000},
          {selector: ['issue', 'status'], op: 'IN', value: [1, 2, 3, 4, 5]},
        ],
      },
      reqB: {
        id: 5,
        type: 'pull',
        hoistedConditions: [
          {selector: ['issue', 'id'], op: '>=', value: 'ysHxepAl0J_dup6'},
          {selector: ['issue', 'modified'], op: '=', value: 5004533700000},
          {selector: ['issue', 'status'], op: 'IN', value: [1, 2, 3, 4, 5]},
        ],
      },
      expected: {
        hoistedConditions: [
          {
            op: '>=',
            selector: ['issue', 'modified'],
            value: 5004533700000,
          },
        ],
        id: 5,
        type: 'pull',
      },
    },
  ] as const)('$name', ({reqA, reqB, expected}) => {
    expect(mergeRequests(reqA, reqB)).toEqual(expected);
  });
});
