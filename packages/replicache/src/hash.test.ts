import {expect} from 'chai';
import * as valita from 'shared/src/valita.js';
import {
  Hash,
  STRING_LENGTH,
  emptyHash,
  fakeHash,
  hashSchema,
  isHash,
  makeNewFakeHashFunction,
  newUUIDHash,
  parse,
} from './hash.js';

const emptyUUID = '00000000-0000-4000-8000-000000000000';

function hashes() {
  return [
    newUUIDHash(),
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

test.skip('type checking only', () => {
  {
    const h = newUUIDHash();
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
    expect(f()).to.equal('a0000000000040008000000000000000' + '000000000000');
    expect(f()).to.equal('a0000000000040008000000000000000' + '000000000001');
    expect(f()).to.equal('a0000000000040008000000000000000' + '000000000002');
  }
  {
    const f = makeNewFakeHashFunction('b');
    expect(f()).to.equal('b0000000000040008000000000000000' + '000000000000');
    expect(f()).to.equal('b0000000000040008000000000000000' + '000000000001');
    expect(f()).to.equal('b0000000000040008000000000000000' + '000000000002');
  }
  {
    const f = makeNewFakeHashFunction();
    expect(f()).to.equal('face0000000040008000000000000000' + '000000000000');
    expect(f()).to.equal('face0000000040008000000000000000' + '000000000001');
    expect(f()).to.equal('face0000000040008000000000000000' + '000000000002');
  }
  {
    const f = makeNewFakeHashFunction('');
    expect(f()).to.equal('00000000000040008000000000000000' + '000000000000');
    expect(f()).to.equal('00000000000040008000000000000000' + '000000000001');
    expect(f()).to.equal('00000000000040008000000000000000' + '000000000002');
  }
  expect(() => makeNewFakeHashFunction('x')).to.throw();
  expect(() => makeNewFakeHashFunction('000000000')).to.throw();
});

test('fakeHash', () => {
  expect(String(fakeHash('aa')).length).to.equal(STRING_LENGTH);
  expect(fakeHash('aa')).to.equal(fakeHash('aa'));
  expect(fakeHash('aa')).to.equal(
    'face0000000040008000000000000000' + '0000000000aa',
  );
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
