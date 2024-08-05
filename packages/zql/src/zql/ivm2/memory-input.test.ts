import {expect, test} from 'vitest';
import {MemoryInput} from './memory-input.js';
import {CaptureOutput} from './capture-output.js';
import {Input, everything} from './operator.js';
import type {Change, TreeDiff} from './tree-diff.js';

test('push/pull', () => {
  const ms = new MemoryInput([
    ['v', 'desc'],
    ['id', 'asc'],
  ]);

  const cv = new CaptureOutput();
  ms.setOutput(cv);

  ms.push([
    {type: 'add', row: {id: 1, v: 'a'}},
    {type: 'add', row: {id: 2, v: 'a'}},
    {type: 'add', row: {id: 3, v: 'b'}},
    {type: 'add', row: {id: 4, v: 'c'}},
    {type: 'remove', row: {id: 3, v: 'b'}},
  ]);

  // View should have received changes in order we sent.
  expect(cv.changes).toEqual([
    {type: 'add', row: {id: 1, v: 'a'}},
    {type: 'add', row: {id: 2, v: 'a'}},
    {type: 'add', row: {id: 3, v: 'b'}},
    {type: 'add', row: {id: 4, v: 'c'}},
    {type: 'remove', row: {id: 3, v: 'b'}},
  ]);

  // Pull should see rows in correct sorted order.
  expect([...ms.pull(everything).diff.changes]).toEqual([
    {type: 'add', row: {id: 4, v: 'c'}},
    {type: 'add', row: {id: 1, v: 'a'}},
    {type: 'add', row: {id: 2, v: 'a'}},
  ]);
});

test('pull during push', () => {
  const ms = new MemoryInput([
    ['v', 'desc'],
    ['id', 'asc'],
  ]);

  const pulled: Change[][] = [];
  const output = {
    push: (input: Input, diff: TreeDiff) => {
      for (const _ of diff.changes) {
        pulled.push([...input.pull(everything).diff.changes]);
      }
    },
  };
  ms.setOutput(output);

  ms.push([
    {type: 'add', row: {id: 1, v: 'a'}},
    {type: 'add', row: {id: 2, v: 'a'}},
    {type: 'remove', row: {id: 1, v: 'a'}},
    {type: 'remove', row: {id: 2, v: 'a'}},
  ]);

  // It's important for correctness that nodes can only "see" via pull what has
  // been pushed so far.
  expect(pulled).toEqual([
    [{type: 'add', row: {id: 1, v: 'a'}}],
    [
      {type: 'add', row: {id: 1, v: 'a'}},
      {type: 'add', row: {id: 2, v: 'a'}},
    ],
    [
      {type: 'add', row: {id: 1, v: 'a'}},
      {type: 'add', row: {id: 2, v: 'a'}},
    ],
    [{type: 'add', row: {id: 2, v: 'a'}}],
  ]);
});
