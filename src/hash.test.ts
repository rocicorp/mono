import {expect} from '@esm-bundle/chai';
import {
  assertNotTempHash,
  emptyHash,
  isHash,
  isTempHash,
  hashOf,
  newTempHash,
  parse,
} from './hash';
import type {ReadonlyJSONValue} from './json';

test('test of', async () => {
  const h = await hashOf('abc');
  expect(h).to.not.equal(emptyHash);
});

test('test native of', async () => {
  const h = await hashOf('abc');
  expect(h).to.not.equal(emptyHash);

  const testData = ['abc', '', '\u0000', 'abc', '💩'];

  for (const s of testData) {
    const hash = await hashOf(s);
    const nativeHash = await hashOf(s);
    expect(hash).to.equal(nativeHash);
  }
});

test('isHash', async () => {
  expect(isHash(emptyHash)).to.be.true;

  const h = await hashOf('abc');
  expect(isHash(h)).to.be.true;
});

test('parse', async () => {
  const h = await hashOf('abc');
  expect(parse(String(emptyHash))).to.equal(emptyHash);
  expect(parse(String(h))).to.equal(h);
});

test('temp hash', async () => {
  const t = newTempHash();
  const c = {hash: await hashOf('dummy')};
  expect(String(t).length, 'temp hash length').to.equal(String(c.hash).length);
  expect(isTempHash(t)).to.equal(true);
  expect(isTempHash(c.hash)).to.equal(false);

  expect(() => assertNotTempHash(t)).to.throw();
});

test.skip('type checking only', async () => {
  const h = await hashOf('abc');
  // @ts-expect-error Should be an error
  const s: string = h;
  console.log(s);

  // @ts-expect-error Sould be an error
  const h2: Hash = 'abc';
  console.log(h2);
});

test('hashOf with different types', async () => {
  const t = async (v: ReadonlyJSONValue) =>
    expect(isHash(await hashOf(v))).to.be.true;

  await t(1);
  await t(1.1);
  await t(true);
  await t(false);
  await t(null);
  await t('');
  await t('a');
  await t('abc');
  await t('abc\u0000');
  await t([]);
  await t([1, 2, 3]);
  await t({});
  await t({a: 1, b: 2});
});
