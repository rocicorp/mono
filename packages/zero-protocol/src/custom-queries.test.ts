import {expect, test} from 'vitest';
import * as v from '../../shared/src/valita.ts';
import {transformResponseMessageSchema} from './custom-queries.ts';

test('parses legacy transformed response', () => {
  expect(
    v.parse(
      ['transformed', [{id: 'q1', name: 'users', ast: {table: 'users'}}]],
      transformResponseMessageSchema,
    ),
  ).toEqual([
    'transformed',
    [{id: 'q1', name: 'users', ast: {table: 'users'}}],
  ]);
});

test('parses transformed response with principal metadata', () => {
  expect(
    v.parse(
      [
        'transformed',
        [{id: 'q1', name: 'users', ast: {table: 'users'}}],
        {principalID: 'principal-1'},
      ],
      transformResponseMessageSchema,
    ),
  ).toEqual([
    'transformed',
    [{id: 'q1', name: 'users', ast: {table: 'users'}}],
    {principalID: 'principal-1'},
  ]);
});

test('parses transformed response with null principal metadata', () => {
  expect(
    v.parse(
      [
        'transformed',
        [{id: 'q1', name: 'users', ast: {table: 'users'}}],
        {principalID: null},
      ],
      transformResponseMessageSchema,
    ),
  ).toEqual([
    'transformed',
    [{id: 'q1', name: 'users', ast: {table: 'users'}}],
    {principalID: null},
  ]);
});

test('rejects transformed response metadata with invalid principal type', () => {
  expect(() =>
    v.parse(
      [
        'transformed',
        [{id: 'q1', name: 'users', ast: {table: 'users'}}],
        {principalID: 1},
      ],
      transformResponseMessageSchema,
    ),
  ).toThrow();
});
