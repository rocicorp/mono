import {expect, test} from 'vitest';
import {Debug} from './debug-delegate.ts';

const q = 'SELECT * FROM users';

test('keeps all vended rows by default', () => {
  const debug = new Debug(true);
  debug.rowVended('users', q, {id: 1});
  debug.rowVended('users', q, {id: 2});
  expect(debug.getVendedRows()).toEqual({users: {[q]: [{id: 1}, {id: 2}]}});
  expect(debug.getVendedRowCounts()).toEqual({users: {[q]: 2}});
});

test('maxRowsPerQuery caps kept rows but not counts', () => {
  const debug = new Debug(true, 1);
  debug.rowVended('users', q, {id: 1});
  debug.rowVended('users', q, {id: 2});
  expect(debug.getVendedRows()).toEqual({users: {[q]: [{id: 1}]}});
  expect(debug.getVendedRowCounts()).toEqual({users: {[q]: 2}});
});

test('collectRows false keeps no rows but still counts', () => {
  const debug = new Debug(false);
  debug.rowVended('users', q, {id: 1});
  expect(debug.getVendedRows()).toEqual({users: {}});
  expect(debug.getVendedRowCounts()).toEqual({users: {[q]: 1}});
});
