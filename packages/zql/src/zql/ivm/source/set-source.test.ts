import fc from 'fast-check';
import {expect, test} from 'vitest';
import type {Listener} from '../graph/difference-stream.js';
import {Materialite} from '../materialite.js';

type E = {id: number};

const ordering = [[['test', 'id']], 'asc'] as const;
const descOrdering = [[['test', 'id']], 'desc'] as const;
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

test('replace', async () => {
  await fc.assert(
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
        order: [[['e2', 'id']], 'asc'],
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
          [
            ['e2', 'x'],
            ['e2', 'id'],
          ],
          'asc',
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
          [
            ['e2', 'x'],
            ['e2', 'id'],
          ],
          'asc',
        ],
        hoistedConditions: [],
      },
      listener,
    );
  });

  expect(items).toEqual(
    baseItems.concat({id: 4, x: 'd'}).sort((l, r) => l.x.localeCompare(r.x)),
  );
});
