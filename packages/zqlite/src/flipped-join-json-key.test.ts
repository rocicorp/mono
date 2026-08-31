import {expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Catch, type CaughtNode} from '../../zql/src/ivm/catch.ts';
import {FlippedJoin} from '../../zql/src/ivm/flipped-join.ts';
import {makeSourceChangeAdd} from '../../zql/src/ivm/source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import {createSource} from './test/source-factory.ts';

// FlippedJoin renders a json join-key value as `'j' + JSON.stringify(v)` to
// key its child/parent maps. A `json` column can hold a plain string too, so
// that rendering is itself a possible column value: without a tag on strings,
// the literal `'j{"a":1}'` shares a bucket with the object `{a: 1}` and the
// two get joined to each other's rows.
//
// The fixture lives here rather than beside the other flipped-join tests
// because those run against MemorySource as well, and `compareValues` cannot
// order a json column that holds both an object and a string.

const lc = createSilentLogContext();
const jsonObject = {a: 1};
const tagTwin = 'j{"a":1}';

function setup() {
  const parent = createSource(
    lc,
    testLogConfig,
    'parent',
    {id: {type: 'string'}, k: {type: 'json'}},
    ['id'],
  );
  const child = createSource(
    lc,
    testLogConfig,
    'child',
    {id: {type: 'string'}, parentK: {type: 'json'}},
    ['id'],
  );
  consume(parent.push(makeSourceChangeAdd({id: 'p1', k: jsonObject})));
  consume(parent.push(makeSourceChangeAdd({id: 'p2', k: tagTwin})));
  consume(child.push(makeSourceChangeAdd({id: 'c1', parentK: jsonObject})));
  consume(child.push(makeSourceChangeAdd({id: 'c2', parentK: tagTwin})));

  const fj = new FlippedJoin({
    parent: parent.connect([['id', 'asc']]),
    child: child.connect([['id', 'asc']]),
    parentKey: ['k'],
    childKey: ['parentK'],
    relationshipName: 'children',
    hidden: false,
    system: 'client',
  });
  return {fj, child};
}

function grouped(nodes: readonly CaughtNode[]) {
  return nodes.map(node => {
    if (node === 'yield') {
      throw new Error('unexpected yield in catch result');
    }
    return [
      node.row.id,
      node.relationships.children.map(c => {
        if (c === 'yield') {
          throw new Error('unexpected yield in catch result');
        }
        return c.row.id;
      }),
    ] as const;
  });
}

test('a json object and the string spelling its key tag join separately', () => {
  const {fj} = setup();
  expect(grouped(new Catch(fj).fetch({}))).toEqual([
    ['p1', ['c1']],
    ['p2', ['c2']],
  ]);
});

test('a pushed child reaches only the parent holding its own value', () => {
  const {fj, child} = setup();
  const out = new Catch(fj);
  out.fetch({});
  out.reset();

  consume(child.push(makeSourceChangeAdd({id: 'c3', parentK: tagTwin})));

  expect(out.pushes).toEqual([
    {
      type: 'child',
      row: {id: 'p2', k: tagTwin},
      child: {
        relationshipName: 'children',
        change: {
          type: 'add',
          node: {row: {id: 'c3', parentK: tagTwin}, relationships: {}},
        },
      },
    },
  ]);
  expect(grouped(new Catch(fj).fetch({}))).toEqual([
    ['p1', ['c1']],
    ['p2', ['c2', 'c3']],
  ]);
});
