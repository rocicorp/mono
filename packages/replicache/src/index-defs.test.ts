import {expect} from 'chai';
import {
  type IndexDefinition,
  indexDefinitionEqual,
  type IndexDefinitions,
  indexDefinitionsEqual,
} from './index-defs.js';

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
