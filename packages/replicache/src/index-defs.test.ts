/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {expect, test} from 'vitest';
import {
  type IndexDefinition,
  indexDefinitionEqual,
  type IndexDefinitions,
  indexDefinitionsEqual,
} from './index-defs.ts';

test('indexDefinitionsEqual', () => {
  const t = (a: IndexDefinition, b: IndexDefinition = a) => {
    expect(indexDefinitionEqual(a, b)).true;
    expect(indexDefinitionEqual(b, a)).true;
  };
  const f = (a: IndexDefinition, b: IndexDefinition = a) => {
    expect(indexDefinitionEqual(a, b)).false;
    expect(indexDefinitionEqual(b, a)).false;
  };

  t({jsonPointer: ''});
  t({jsonPointer: '', allowEmpty: true});
  t({jsonPointer: '', allowEmpty: false});
  t({jsonPointer: '', prefix: ''});
  t({jsonPointer: '', prefix: '', allowEmpty: true});
  t({jsonPointer: '', prefix: '', allowEmpty: false});

  t({jsonPointer: '/foo'}, {jsonPointer: '/foo', allowEmpty: false});
  t({jsonPointer: '/foo'}, {jsonPointer: '/foo', prefix: ''});

  f({jsonPointer: '/foo'}, {jsonPointer: '/bar'});
  f({jsonPointer: '/foo'}, {jsonPointer: '/foo', allowEmpty: true});
  f({jsonPointer: '/foo'}, {jsonPointer: '/foo', prefix: 'a'});
});

test('indexDefinitionsEqual', () => {
  const t = (a: IndexDefinitions, b: IndexDefinitions = a) => {
    expect(indexDefinitionsEqual(a, b)).true;
    expect(indexDefinitionsEqual(b, a)).true;
  };
  const f = (a: IndexDefinitions, b: IndexDefinitions = a) => {
    expect(indexDefinitionsEqual(a, b)).false;
    expect(indexDefinitionsEqual(b, a)).false;
  };

  t({});
  t({a: {jsonPointer: '/a'}});
  t({a: {jsonPointer: '/a'}, b: {jsonPointer: '/b'}});
  t(
    {a: {jsonPointer: '/a'}, b: {jsonPointer: '/b'}},
    {b: {jsonPointer: '/b'}, a: {jsonPointer: '/a'}},
  );

  f({}, {a: {jsonPointer: '/a'}});
  f({a: {jsonPointer: '/a'}}, {b: {jsonPointer: '/a'}});
});
