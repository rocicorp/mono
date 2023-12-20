import {describe, expect, test} from '@jest/globals';
import {must} from 'shared/src/must.js';
import type {ListOptions} from '../storage/storage.js';
import {createSilentLogContext} from '../util/test-utils.js';
import {listParamsSchema, makeListControl, makeListResults} from './list.js';
import {queryParams} from './router.js';

describe('parse ListOptions', () => {
  type Case = {
    name: string;
    queryString: string;
    maxMaxResults: number;
    listOptions?: ListOptions;
    error?: string;
  };
  const cases: Case[] = [
    {
      name: 'default options (no query)',
      queryString: '',
      maxMaxResults: 100,
      listOptions: {
        start: {
          key: '',
          exclusive: false,
        },
        limit: 101,
      },
    },
    {
      name: 'start key',
      queryString: 'startKey=foo',
      maxMaxResults: 100,
      listOptions: {
        start: {
          key: 'foo',
          exclusive: false,
        },
        limit: 101,
      },
    },
    {
      name: 'start after key, max results',
      queryString: 'maxResults=90&startAfterKey=bar',
      maxMaxResults: 100,
      listOptions: {
        start: {
          key: 'bar',
          exclusive: true,
        },
        limit: 91,
      },
    },
    {
      name: 'limit max results',
      queryString: 'maxResults=200',
      maxMaxResults: 100,
      listOptions: {
        start: {
          key: '',
          exclusive: false,
        },
        limit: 101,
      },
    },
    {
      name: 'disallow both startKey and startAfterKey',
      queryString: 'maxResults=200&startKey=foo&startAfterKey=bar',
      maxMaxResults: 100,
      error:
        '400: Query string error. Cannot specify both startKey and startAfterKey. Got object (request)',
    },
    {
      name: 'bad maxResults',
      queryString: 'maxResults=not-a-number',
      maxMaxResults: 100,
      error:
        '400: Query string error. Expected valid number at maxResults. Got "not-a-number" (request)',
    },
  ];

  cases.forEach(c => {
    test(c.name, async () => {
      try {
        const url = `https://roci.dev/room/monkey?${c.queryString}`;
        const ctx = {
          parsedURL: must(new URLPattern().exec(url)),
          lc: createSilentLogContext(),
        };
        const {query: listParams} = await queryParams(listParamsSchema)(
          ctx,
          new Request(url),
        );
        const listControl = makeListControl(listParams, c.maxMaxResults);
        expect(c.listOptions).not.toBeUndefined;
        expect(listControl.getOptions()).toEqual(c.listOptions);
      } catch (e) {
        expect((e as Error).message).toBe(c.error);
      }
    });
  });
});

test('makeListResponse', () => {
  expect(makeListResults([], 3)).toEqual({
    results: [],
    numResults: 0,
    more: false,
  });
  expect(makeListResults([1, 2], 3)).toEqual({
    results: [1, 2],
    numResults: 2,
    more: false,
  });
  expect(makeListResults([1, 2, 3], 3)).toEqual({
    results: [1, 2, 3],
    numResults: 3,
    more: false,
  });
  expect(makeListResults([1, 2, 3, 4], 3)).toEqual({
    results: [1, 2, 3],
    numResults: 3,
    more: true,
  });
  expect(makeListResults([5, 4, 3, 2, 1], 2)).toEqual({
    results: [5, 4],
    numResults: 2,
    more: true,
  });
});
