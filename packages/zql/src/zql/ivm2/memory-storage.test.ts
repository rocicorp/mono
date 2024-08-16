import {expect, test} from 'vitest';
import {MemoryStorage} from './memory-storage.js';

test('basics', () => {
  const ms = new MemoryStorage();
  expect(ms.get(['foo'])).undefined;
  expect(ms.get(['foo', 'bar'])).undefined;
  expect(ms.get(['foo', 'baz'])).undefined;

  ms.set(['foo'], 'bar');
  ms.set(['foo', 'bar'], 'baz');
  ms.set(['foo', 'baz'], 'monkey');

  expect(ms.get(['foo'])).equal('bar');
  expect(ms.get(['foo', 'bar'])).equal('baz');
  expect(ms.get(['foo', 'baz'])).equal('monkey');

  ms.del(['foo']);
  ms.del(['foo', 'bar']);
  ms.del(['foo', 'baz']);

  expect(ms.get(['foo'])).undefined;
  expect(ms.get(['foo', 'bar'])).undefined;
  expect(ms.get(['foo', 'baz'])).undefined;
});

test('default', () => {
  const ms = new MemoryStorage();
  expect(ms.get(['foo'], 'bar')).equal('bar');
  ms.set(['foo'], 'baz');
  expect(ms.get(['foo'], 'bar')).equal('baz');
});

test('other types', () => {
  const ms = new MemoryStorage();
  ms.set(['foo'], 1);
  ms.set(['bar'], true);
  ms.set(['baz'], null);
  ms.set(['qux'], {a: 1});
  ms.set(['quux'], [1, 2, 3]);

  expect(ms.get(['foo'])).equal(1);
  expect(ms.get(['bar'])).equal(true);
  expect(ms.get(['baz'])).equal(null);
  expect(ms.get(['qux'])).toStrictEqual({a: 1});
  expect(ms.get(['quux'])).toStrictEqual([1, 2, 3]);
});
