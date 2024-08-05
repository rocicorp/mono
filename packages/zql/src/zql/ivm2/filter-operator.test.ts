import {expect, test} from 'vitest';
import {MemoryInput} from './memory-input.js';
import {FilterOperator} from './filter-operator.js';
import {CaptureOutput} from './capture-output.js';
import {everything} from './operator.js';
import type {SimpleOperator} from '../ast2/ast.js';
import type {Value} from './data.js';

test('basics', () => {
  const mem = new MemoryInput([['id', 'asc']]);
  const filter = new FilterOperator(mem, {
    field: 'f',
    op: '=',
    value: 1,
  });
  mem.setOutput(filter);

  const capture = new CaptureOutput();
  filter.setOutput(capture);

  mem.push([
    {type: 'add', row: {id: 1, f: 1}},
    {type: 'add', row: {id: 2, f: 2}},
    {type: 'add', row: {id: 3, f: 1}},
    {type: 'remove', row: {id: 1, f: 1}},
  ]);

  // The output should only have received the changes that match the filter.
  expect(capture.changes).toEqual([
    {type: 'add', row: {id: 1, f: 1}},
    {type: 'add', row: {id: 3, f: 1}},
    {type: 'remove', row: {id: 1, f: 1}},
  ]);

  // Pulling should only return rows that currently match filter.
  expect([...filter.pull(everything).diff]).toEqual([
    {type: 'add', row: {id: 3, f: 1}},
  ]);

  // But if we look at mem, we see the non-matching rows are still there.
  expect([...mem.pull(everything).diff]).toEqual([
    {type: 'add', row: {id: 2, f: 2}},
    {type: 'add', row: {id: 3, f: 1}},
  ]);
});

test('operators', () => {
  // TODO(aa): This require some thought on how to exhaustively test.
  // fast-check to the rescue?
  const pipe = (op: SimpleOperator, value: Value) => {
    const mem = new MemoryInput([['id', 'asc']]);
    const filter = new FilterOperator(mem, {field: 'f', op, value});
    mem.setOutput(filter);
    const capture = new CaptureOutput();
    filter.setOutput(capture);
    return {mem, filter, capture};
  };

  // equals
  const vals = [null, true, false, -1, 0, 1, 3.14, '', 'a', 'b'];
  for (const v1 of vals) {
    for (const v2 of vals) {
      const {mem: m1, capture: c1} = pipe('=', v1);
      m1.push([{type: 'add', row: {id: 1, f: v2}}]);
      expect([...c1.changes]).toEqual(
        v1 === v2 ? [{type: 'add', row: {id: 1, f: v1}}] : [],
      );

      // reverse
      const {mem: m2, capture: c2} = pipe('=', v2);
      m2.push([{type: 'add', row: {id: 1, f: v1}}]);
      expect([...c2.changes]).toEqual(
        v1 === v2 ? [{type: 'add', row: {id: 1, f: v2}}] : [],
      );
    }
  }
});
