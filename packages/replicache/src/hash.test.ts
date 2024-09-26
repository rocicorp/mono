import {expect} from 'chai';
import * as valita from 'shared/src/valita.js';
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
} from './hash.js';

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
  expect(isHash(emptyHash)).to.be.true;

  for (const h of hashes()) {
    expect(isHash(h)).to.be.true;
    expect(isHash(h + 'a')).to.be.true;
    expect(isHash(String(h).slice(0, -1))).to.be.true;
  }
});

test('parse', () => {
  for (const h of hashes()) {
    expect(parse(String(emptyHash))).to.equal(emptyHash);
    expect(parse(String(h))).to.equal(h);
    expect(parse(h + 'a')).to.equal(h + 'a');
    expect(parse(String(h).slice(0, -1))).to.equal(String(h).slice(0, -1));
  }
});

test('newRandomHash', () => {
  const h = newRandomHash();
  expect(h.length).to.equal(22);
  expect(h).to.match(/^[0-9a-v-]+$/);
});

test.skip('type checking only', () => {
  {
    const h = newRandomHash();
    // Should not be an error
    const s: string = h;
    console.log(s);

    // @ts-expect-error Should be an error
    const h2: Hash = 'abc';
    console.log(h2);
  }
});

test('makeNewFakeHashFunction', () => {
  {
    const f = makeNewFakeHashFunction('a');
    expect(f()).to.equal('a000000000000000000000');
    expect(f()).to.equal('a000000000000000000001');
    expect(f()).to.equal('a000000000000000000002');
  }
  {
    const f = makeNewFakeHashFunction('b');
    expect(f()).to.equal('b000000000000000000000');
    expect(f()).to.equal('b000000000000000000001');
    expect(f()).to.equal('b000000000000000000002');
  }
  {
    const f = makeNewFakeHashFunction();
    expect(f()).to.equal('fake000000000000000000');
    expect(f()).to.equal('fake000000000000000001');
    expect(f()).to.equal('fake000000000000000002');
  }
  {
    const f = makeNewFakeHashFunction('');
    expect(f()).to.equal('0000000000000000000000');
    expect(f()).to.equal('0000000000000000000001');
    expect(f()).to.equal('0000000000000000000002');
  }
  expect(() => makeNewFakeHashFunction('x')).to.throw();
  expect(() => makeNewFakeHashFunction('000000000')).to.throw();
});

test('fakeHash', () => {
  expect(String(fakeHash('aa')).length).to.equal(STRING_LENGTH);
  expect(fakeHash('aa')).to.equal(fakeHash('aa'));
  expect(fakeHash('aa')).to.equal('fake0000000000000000aa');
});

test('valita schema', () => {
  for (const h of hashes()) {
    expect(valita.is(h, hashSchema)).to.be.true;
  }
  expect(valita.is('xyz', hashSchema)).to.be.false;

  for (const h of hashes()) {
    expect(() => valita.assert(h, hashSchema)).not.to.throw();
  }
  expect(() => valita.assert('xyz', hashSchema)).to.throw(TypeError);
  expect(() => valita.assert('xyz', hashSchema)).to.throw(
    'Invalid hash. Got "xyz"',
  );
});
