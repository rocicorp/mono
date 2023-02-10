import {LogContext} from '@rocicorp/logger';
import {expect} from '@esm-bundle/chai';
import type {JSONValue} from '../json';
import {stringCompare} from '../string-compare';
import * as dag from '../dag/mod';
import {
  decodeIndexKey,
  encodeIndexKey,
  encodeIndexScanKey,
  evaluateJSONPointer,
  getIndexKeys,
  IndexKey,
  IndexOperation,
  indexValue,
  KEY_SEPARATOR,
  KEY_VERSION_0,
} from './index';
import {BTreeWrite} from '../btree/mod';
import {asyncIterableToArray} from '../async-iterable-to-array';

test('test index key', () => {
  const testValid = (secondary: string, primary: string) => {
    // Ensure the encoded value is what we expect.
    const encoded = encodeIndexKey([secondary, primary]);
    expect(KEY_VERSION_0).to.equal(encoded.slice(0, KEY_VERSION_0.length));
    const secondaryIndex = KEY_VERSION_0.length;
    const separatorIndex = secondaryIndex + secondary.length;
    expect(encoded.slice(secondaryIndex, separatorIndex)).to.equal(secondary);
    const primaryIndex = separatorIndex + KEY_SEPARATOR.length;
    expect(encoded.slice(separatorIndex, primaryIndex)).to.equal(KEY_SEPARATOR);
    expect(encoded.slice(primaryIndex)).to.equal(primary);

    // Ensure we can decode it properly.
    const decoded = decodeIndexKey(encoded);
    expect(decoded[0]).to.equal(secondary);
    expect(decoded[1]).to.equal(primary);
  };

  testValid('', '');
  testValid('', '\u0000');
  testValid('', '\u0001');
  testValid('a', '');
  testValid('a', 'a');
  testValid('foo', '\u0001\u0002\u0003');

  const testInvalidEncode = (
    secondary: string,
    primary: string,
    expected: string,
  ) => {
    expect(() => encodeIndexKey([secondary, primary])).to.throw(
      Error,
      expected,
    );
  };
  testInvalidEncode(
    'no \0 nulls',
    '',
    'Secondary key cannot contain null byte',
  );

  const testInvalidDecode = (encoded: string, expected: string) => {
    expect(() => decodeIndexKey(encoded)).to.throw(Error, expected);
  };
  testInvalidDecode('', 'Invalid version');
  testInvalidDecode('\u0001', 'Invalid version');
  testInvalidDecode('\u0000', 'Invalid formatting');
  testInvalidDecode('\u0000\u0001\u0002', 'Invalid formatting');
});

test('encode scan key', () => {
  const t = (secondary: string, primary: string) => {
    const encodedIndexKey = encodeIndexKey([secondary, primary]);
    const scanKey = encodeIndexScanKey(secondary, primary);

    expect(scanKey.startsWith(encodedIndexKey)).to.be.true;

    expect(stringCompare(encodedIndexKey, scanKey)).to.greaterThanOrEqual(0);
  };

  t('', '');
  t('', '\u0000');
  t('', '\u0001');
  t('foo', '');
  t('foo', '\u0000');
  t('foo', '\u0001');
});

test('index key sort', () => {
  const t = (left: IndexKey, right: IndexKey) => {
    const a = encodeIndexKey(left);
    const b = encodeIndexKey(right);
    expect(stringCompare(a, b)).to.equal(-1);
  };

  t(['', ''], ['', '\u0000']);
  t(['', '\u0000'], ['a', '']);
  t(['a', '\u0000'], ['aa', '']);
  t(['A', ''], ['a', '']);
  t(['foo', ''], ['foobar', '']);
  t(['😀', ''], ['😜', '']);
  t(['a', '\u00ff'], ['aa', '\u0000']);
});

// By design the index key is encoded in a way that doesn't permit collisions,
// eg a situation where scan({indexName: "...", ...prefix="foo"}) matches a
// value with secondary index "f" and primary index "oo". This test gives us a
// tiny extra assurance that this is the case.
test('index key uniqueness', () => {
  const t = (left: IndexKey, right: IndexKey) => {
    const a = encodeIndexKey(left);
    const b = encodeIndexKey(right);
    expect(stringCompare(a, b)).to.not.equal(0);
  };

  t(['', '\u0061'], ['a', '']);
});

test('get index keys', () => {
  const t = (
    key: string,
    input: JSONValue,
    jsonPointer: string,
    expected: IndexKey[] | string | RegExp,
  ) => {
    if (Array.isArray(expected)) {
      const keys = getIndexKeys(key, input, jsonPointer, false);
      expect(keys).to.deep.equal(expected.map(k => encodeIndexKey(k)));
    } else {
      expect(() => getIndexKeys(key, input, jsonPointer, false)).to.throw(
        expected,
      );
    }
  };

  // no matching target
  t('k', {}, '/foo', 'No value at path: /foo');

  // unsupported target types
  t('k', {unsupported: {}}, '/unsupported', 'Unsupported target type');
  t('k', {unsupported: null}, '/unsupported', 'Unsupported target type');
  t('k', {unsupported: true}, '/unsupported', 'Unsupported target type');
  t('k', {unsupported: 42}, '/unsupported', 'Unsupported target type');
  t('k', {unsupported: 88.8}, '/unsupported', 'Unsupported target type');
  t('k', 'no \0 allowed', '', 'Secondary key cannot contain null byte');

  // success
  // array of string
  t('k', {foo: []}, '/foo', []);
  t('k', {foo: ['bar', '', 'baz']}, '/foo', [
    ['bar', 'k'],
    ['', 'k'],
    ['baz', 'k'],
  ]);

  // string
  t('foo', {foo: 'bar'}, '/foo', [['bar', 'foo']]);
  t('foo', {foo: {bar: ['hot', 'dog']}}, '/foo/bar/1', [['dog', 'foo']]);
  t('', {foo: 'bar'}, '/foo', [['bar', '']]);
  t('/! ', {foo: 'bar'}, '/foo', [['bar', '/! ']]);
});

test('json pointer', () => {
  for (const v of [null, 42, true, false, [], {}, 'foo']) {
    expect(() => evaluateJSONPointer(null, 'x')).to.throw(
      'Invalid JSON pointer',
    );

    expect(evaluateJSONPointer(v, '')).to.equal(v);
    expect(evaluateJSONPointer(v, '/')).to.equal(undefined);
    expect(evaluateJSONPointer(v, '/a')).to.equal(undefined);
  }

  expect(evaluateJSONPointer({a: 1}, '/a')).to.equal(1);
  expect(evaluateJSONPointer({a: {b: 2}}, '/a')).to.deep.equal({b: 2});
  expect(evaluateJSONPointer({a: {b: 3}}, '/a/b')).to.equal(3);
  expect(evaluateJSONPointer({a: {b: 4}}, '/a/')).to.equal(undefined);

  expect(evaluateJSONPointer('hi', '/length')).to.equal(undefined);

  expect(evaluateJSONPointer(['a', 'b'], '/0')).to.equal('a');
  expect(evaluateJSONPointer(['a', 'b'], '/1')).to.equal('b');
  expect(evaluateJSONPointer(['a', 'b'], '/00')).to.equal(undefined);
  expect(evaluateJSONPointer(['a', 'b'], '/01')).to.equal(undefined);
  expect(evaluateJSONPointer(['a', 'b'], '/2')).to.equal(undefined);
});

test('index value', async () => {
  const t = async (
    key: string,
    value: JSONValue,
    jsonPointer: string,
    op: IndexOperation,
    expected: number[] | string,
  ) => {
    const dagStore = new dag.TestStore();
    await dagStore.withWrite(async dagWrite => {
      const index = new BTreeWrite(dagWrite);
      await index.put(encodeIndexKey(['s1', '1']), 'v1');
      await index.put(encodeIndexKey(['s2', '2']), 'v2');

      if (Array.isArray(expected)) {
        await indexValue(new LogContext(), index, op, key, value, jsonPointer);

        const actualVal = await asyncIterableToArray(index.entries());
        expect(expected.length).to.equal(actualVal.length);
        for (let i = 0; i < expected.length; i++) {
          const expEntry = encodeIndexKey([
            `s${expected[i]}`,
            `${expected[i]}`,
          ]);
          expect(expEntry).to.deep.equal(actualVal[i][0]);
          expect(await index.get(expEntry)).to.deep.equal(actualVal[i][1]);
        }
      } else {
        expect(() =>
          indexValue(new LogContext(), index, op, key, value, jsonPointer),
        ).to.throw(expected);
      }
    });
  };

  await t('3', {s: 's3', v: 'v3'}, '/s', IndexOperation.Add, [1, 2, 3]);
  await t('1', {s: 's1', v: 'v1'}, '/s', IndexOperation.Remove, [2]);
});

test(`decodeIndexKey`, () => {
  expect(decodeIndexKey('\u0000abc\u0000def')).to.deep.equal(['abc', 'def']);
  expect(decodeIndexKey('\u0000abc\u0000')).to.deep.equal(['abc', '']);
  expect(decodeIndexKey('\u0000\u0000def')).to.deep.equal(['', 'def']);

  expect(() => decodeIndexKey('abc')).to.throw('Invalid version');
  expect(() => decodeIndexKey('\u0000abc')).to.throw('Invalid formatting');
});
