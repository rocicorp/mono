import {expect, test} from 'vitest';
import {joinSymbol} from '../types.js';
import {getOrLiftValue, getValueFromEntity} from './util.js';

test('getValueFromEntity#normalSelect', () => {
  const entity = {
    b: 1,
    c: 2,
  };
  const qualifiedColumn = ['a', 'b'] as const;
  const result = getValueFromEntity(entity, qualifiedColumn);
  expect(result).toEqual(1);
});

test('getValueFromEntity#joinResult', () => {
  const entity = {
    [joinSymbol]: true,
    a: {
      b: 1,
      c: 2,
    },
  };
  const qualifiedColumn = ['a', 'b'] as const;
  const result = getValueFromEntity(entity, qualifiedColumn);
  expect(result).toEqual(1);
});

test('getValueFromEntity: joinResult with *', () => {
  const entity = {
    [joinSymbol]: true,
    a: {
      b: 1,
      c: 2,
    },
  };
  const qualifiedColumn = ['a', '*'] as const;
  const result = getValueFromEntity(entity, qualifiedColumn);
  expect(result).toEqual(entity.a);
});

test('lift field from array', () => {
  const entities = [{b: 1}, {b: 2}];
  const result = getOrLiftValue(entities, 'b');
  expect(result).toEqual([1, 2]);
});
