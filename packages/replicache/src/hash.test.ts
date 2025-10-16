import {expect, test} from 'vitest';
import * as valita from '../../shared/src/valita.ts';
import {
  type Hash,
  STRING_LENGTH,
  emptyHash,
  fakeHash,
  hashSchema,
  isHash,
  makeNewFakeHashFunction,
  newRandomHash,
  parse,
} from './hash.ts';

const emptyUUID = '00000000-0000-4000-8000-000000000000';

function hashes() {
  return [
    newRandomHash(),
    fakeHash(''),
    fakeHash('a'),
    // old native hashes
    '0123456789abcdefghijklmnopqrstuv',
    // old uuid hashes
    '23691827-e581-46b9-afb7-f764938b66c1',
    emptyUUID,
  ];
}

test('isHash', () => {
  expect(isHash(emptyHash)).toBe(true);

  for (const h of hashes()) {
    expect(isHash(h)).toBe(true);
    expect(isHash(h + 'a')).toBe(true);
    expect(isHash(String(h).slice(0, -1))).toBe(true);
  }
});

test('parse', () => {
  for (const h of hashes()) {
    expect(parse(String(emptyHash))).toBe(emptyHash);
    expect(parse(String(h))).toBe(h);
    expect(parse(h + 'a')).toBe(h + 'a');
    expect(parse(String(h).slice(0, -1))).toBe(String(h).slice(0, -1));
  }
});

test('newRandomHash', () => {
  const h = newRandomHash();
  expect(h.length).toBe(22);
  expect(h).toMatch(/^[0-9a-v-]+$/);
});

test.skip('type checking only', () => {
  {
    const h = newRandomHash();
    // Should not be an error
    const s: string = h;

    // @ts-expect-error Should be an error
    const h2: Hash = 'abc';

    return s + h2;
  }
});

test('makeNewFakeHashFunction', () => {
  {
    const f = makeNewFakeHashFunction('a');
    expect(f()).toBe('a000000000000000000000');
    expect(f()).toBe('a000000000000000000001');
    expect(f()).toBe('a000000000000000000002');
  }
  {
    const f = makeNewFakeHashFunction('b');
    expect(f()).toBe('b000000000000000000000');
    expect(f()).toBe('b000000000000000000001');
    expect(f()).toBe('b000000000000000000002');
  }
  {
    const f = makeNewFakeHashFunction();
    expect(f()).toBe('fake000000000000000000');
    expect(f()).toBe('fake000000000000000001');
    expect(f()).toBe('fake000000000000000002');
  }
  {
    const f = makeNewFakeHashFunction('');
    expect(f()).toBe('0000000000000000000000');
    expect(f()).toBe('0000000000000000000001');
    expect(f()).toBe('0000000000000000000002');
  }
  expect(() => makeNewFakeHashFunction('x')).toThrow();
  expect(() => makeNewFakeHashFunction('000000000')).toThrow();
});

test('fakeHash', () => {
  expect(String(fakeHash('aa')).length).toBe(STRING_LENGTH);
  expect(fakeHash('aa')).toBe(fakeHash('aa'));
  expect(fakeHash('aa')).toBe('fake0000000000000000aa');
});

test('valita schema', () => {
  for (const h of hashes()) {
    expect(valita.is(h, hashSchema)).toBe(true);
  }
  expect(valita.is('xyz', hashSchema)).toBe(false);

  for (const h of hashes()) {
    expect(() => valita.assert(h, hashSchema)).not.toThrow();
  }
  expect(() => valita.assert('xyz', hashSchema)).toThrow(TypeError);
  expect(() => valita.assert('xyz', hashSchema)).toThrow(
    'Invalid hash. Got "xyz"',
  );
});
