import {expect, test} from 'vitest';
import {mapAllEntries, mapEntries, mapValues} from './objects.ts';

// Use JSON.stringify in expectations to preserve / verify key order.
const stringify = (o: unknown) => JSON.stringify(o, null, 2);

test('mapValues', () => {
  const obj = {
    foo: 'bar',
    bar: 'baz',
    boo: 'doo',
  };

  expect(stringify(mapValues(obj, v => v.toUpperCase())))
    .toMatchInlineSnapshot(`
    "{
      "foo": "BAR",
      "bar": "BAZ",
      "boo": "DOO"
    }"
  `);
});

test('mapEntries', () => {
  const obj = {
    boo: 'doo',
    foo: 'bar',
    bar: 'baz',
  };

  expect(stringify(mapEntries(obj, (k, v) => [v, k]))).toMatchInlineSnapshot(`
    "{
      "doo": "boo",
      "bar": "foo",
      "baz": "bar"
    }"
  `);
});

test('mapAllEntries', () => {
  const obj = {
    foo: 'bar',
    bar: 'baz',
    boo: 'doo',
  };

  const sorted = mapAllEntries(obj, e =>
    e.sort(([a], [b]) => a.localeCompare(b)),
  );
  expect(stringify(sorted)).toMatchInlineSnapshot(`
    "{
      "bar": "baz",
      "boo": "doo",
      "foo": "bar"
    }"
  `);

  const reversed = mapAllEntries(obj, e =>
    e.sort(([a], [b]) => a.localeCompare(b) * -1),
  );
  expect(stringify(reversed)).toMatchInlineSnapshot(`
    "{
      "foo": "bar",
      "boo": "doo",
      "bar": "baz"
    }"
  `);
});
