import {expect} from 'chai';
import {deepFreeze, isDeepFrozen} from './frozen-json.js';
import type {ReadonlyJSONValue} from './json.js';

test('toDeepFrozen', () => {
  expect(deepFreeze(null)).to.equal(null);
  expect(deepFreeze(true)).to.equal(true);
  expect(deepFreeze(false)).to.equal(false);
  expect(deepFreeze(1)).to.equal(1);
  expect(deepFreeze(123.456)).to.equal(123.456);
  expect(deepFreeze('')).to.equal('');
  expect(deepFreeze('abc')).to.equal('abc');

  const expectSameObject = (v: ReadonlyJSONValue) => {
    expect(deepFreeze(v)).to.equal(v);
  };

  const expectFrozen = (v: ReadonlyJSONValue) => {
    expectSameObject(v);
    expect(v).frozen;
  };

  expectFrozen([]);
  expectFrozen([1, 2, 3]);
  expectFrozen({});
  expectFrozen({a: 1, b: 2});
  expectFrozen({a: 1, b: 2, c: [3, 4, 5]});

  {
    const o = [0, 1, {a: 2, b: 3, c: [4, 5, 6]}] as const;
    const o2 = deepFreeze(o);
    expect(o2).equal(o);
    expect(o2).frozen;
    expect(o[2]).frozen;
    expect(o[2].c).frozen;
  }

  {
    const o = {a: undefined};
    const o2 = deepFreeze(o);
    expect(o2).equal(o);
    expect(o2).frozen;
  }
});

test('isDeepFrozen', () => {
  expect(isDeepFrozen(null, [])).to.be.true;
  expect(isDeepFrozen(true, [])).to.be.true;
  expect(isDeepFrozen(1, [])).to.be.true;
  expect(isDeepFrozen('abc', [])).to.be.true;

  expect(isDeepFrozen([], [])).to.be.false;
  expect(isDeepFrozen([1, 2, 3], [])).to.be.false;
  expect(isDeepFrozen({}, [])).to.be.false;
  expect(isDeepFrozen({a: 1, b: 2}, [])).to.be.false;
  expect(isDeepFrozen({a: 1, b: 2, c: [3, 4, 5]}, [])).to.be.false;

  const o = [0, 1, {a: 2, b: 3, c: [4, 5, 6]}] as const;
  expect(isDeepFrozen(o, [])).to.be.false;
  expect(isDeepFrozen(o[2], [])).to.be.false;
  expect(isDeepFrozen(o[2].c, [])).to.be.false;
  deepFreeze(o);
  expect(isDeepFrozen(o, [])).to.be.true;
  expect(Object.isFrozen(o)).to.be.true;
  expect(isDeepFrozen(o[2], [])).to.be.true;
  expect(Object.isFrozen(o[2])).to.be.true;
  expect(isDeepFrozen(o[2].c, [])).to.be.true;
  expect(Object.isFrozen(o[2].c)).to.be.true;

  {
    const o = [0, 1, {a: 2, b: 3, c: [4, 5, 6]}] as const;
    expect(isDeepFrozen(o, [])).to.be.false;
    expect(isDeepFrozen(o[2], [])).to.be.false;
    expect(isDeepFrozen(o[2].c, [])).to.be.false;
    Object.freeze(o);
    Object.freeze(o[2]);
    expect(isDeepFrozen(o, [])).to.be.false;
    expect(Object.isFrozen(o)).to.be.true;
    expect(isDeepFrozen(o[2], [])).to.be.false;
    expect(Object.isFrozen(o[2])).to.be.true;
    expect(isDeepFrozen(o[2].c, [])).to.be.false;
    expect(Object.isFrozen(o[2].c)).to.be.false;

    Object.freeze(o[2].c);
    expect(isDeepFrozen(o, [])).to.be.true;
    expect(Object.isFrozen(o)).to.be.true;
    expect(isDeepFrozen(o[2], [])).to.be.true;
    expect(Object.isFrozen(o[2])).to.be.true;
    expect(isDeepFrozen(o[2].c, [])).to.be.true;
    expect(Object.isFrozen(o[2].c)).to.be.true;
  }

  {
    const o = {a: undefined};
    expect(isDeepFrozen(o, [])).to.be.false;
    Object.freeze(o);
    expect(isDeepFrozen(o, [])).to.be.true;
  }
});
