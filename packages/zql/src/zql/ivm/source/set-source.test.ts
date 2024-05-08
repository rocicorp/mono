import fc from 'fast-check';
import {expect, test} from 'vitest';
import {Materialite} from '../materialite.js';

type E = {id: number};

const comparator = (l: E, r: E) => l.id - r.id;
const numberComparator = (l: number, r: number) => l - r;

test('add', () => {
  fc.assert(
    fc.property(fc.uniqueArray(fc.integer()), arr => {
      const m = new Materialite();
      const source = m.newSetSource(comparator);

      arr.forEach(x => source.add({id: x}));
      expect([...source.value]).toEqual(
        arr.sort(numberComparator).map(x => ({id: x})),
      );
    }),
  );
});

test('delete', () => {
  fc.assert(
    fc.property(fc.uniqueArray(fc.integer()), arr => {
      const m = new Materialite();
      const source = m.newSetSource(comparator);

      arr.forEach(x => source.add({id: x}));
      arr.forEach(x => source.delete({id: x}));
      expect([...source.value]).toEqual([]);
    }),
  );
});

test('on', () => {
  const m = new Materialite();
  const source = m.newSetSource(comparator);

  let callCount = 0;
  const dispose = source.on(value => {
    expect(value).toEqual(source.value);
    ++callCount;

    expect([...value]).toEqual([{id: 1}, {id: 2}]);
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
    fc.asyncProperty(fc.uniqueArray(fc.integer()), async arr => {
      const m = new Materialite();
      const source = m.newSetSource(comparator);

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
      await Promise.resolve();

      expect([...source.value]).toEqual(arr.map(id => ({id})).sort(comparator));
    }),
  );
});

// we don't do any rollbacks. If Materialite throws then
// it has diverged from Replicache and we're in a bad state.
test('rollback', async () => {
  const m = new Materialite();
  const source = m.newSetSource(comparator);

  try {
    m.tx(() => {
      source.add({id: 1});
      throw new Error('rollback');
    });
  } catch (e) {
    // ignore
  }
  await Promise.resolve();

  expect([...source.value]).toEqual([]);

  source.add({id: 2});
  await Promise.resolve();
  expect([...source.value]).toEqual([{id: 2}]);
});

test('withNewOrdering - we do not update the derived thing / withNewOrdering is not tied to the original. User must do that.', async () => {
  const m = new Materialite();
  const source = m.newSetSource(comparator);
  const derived = source.withNewOrdering((l, r) => r.id - l.id);

  m.tx(() => {
    source.add({id: 1});
    source.add({id: 2});
  });
  await Promise.resolve();

  expect([...source.value]).toEqual([{id: 1}, {id: 2}]);
  expect([...derived.value]).toEqual([]);
});

test('withNewOrdering - is correctly ordered', async () => {
  const m = new Materialite();

  await fc.assert(
    fc.asyncProperty(fc.uniqueArray(fc.integer()), async arr => {
      const source = m.newSetSource(comparator);
      const derived = source.withNewOrdering((l, r) => r.id - l.id);
      m.tx(() => {
        arr.forEach(id => {
          source.add({id});
          derived.add({id});
        });
      });
      await Promise.resolve();

      expect([...source.value]).toEqual(arr.map(id => ({id})).sort(comparator));
      expect([...derived.value]).toEqual(
        arr.sort((l, r) => r - l).map(id => ({id})),
      );
    }),
  );
});
