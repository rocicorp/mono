import {describe, expect, vi, test} from 'vitest';
import * as serverQueries from '../server/server-queries.ts';
import * as sharedQueries from '../shared/queries.ts';
import {getQuery} from './get-query.ts';
import * as readPermissions from './read-permissions.ts';
import type {NamedQueryImpl} from '@rocicorp/zero';

vi.mock('./read-permissions', () => ({
  applyIssuePermissions: vi.fn(),
}));

const context = {} as serverQueries.ServerContext;

describe('permissions are applied for our sensitive queries', () => {
  const queries = Object.keys(serverQueries).map(name => {
    const args = mockArgs[name as keyof typeof mockArgs];
    if (!args) {
      throw new Error(`No mock args for query: ${name}`);
    }
    return [name, args] as const;
  });

  test.each(queries)('calls applyPermissions for %s', (name, args) => {
    const permissionsSpy = vi.spyOn(readPermissions, 'applyIssuePermissions');
    getQuery(context, name, args);
    expect(permissionsSpy).toHaveBeenCalledOnce();
  });
});

test('export names match between server and shared queries', () => {
  const serverQueryNames = Object.keys(serverQueries);
  const sharedQueryNames = Object.keys(sharedQueries);

  const sharedNames = new Set(sharedQueryNames);
  for (const name of serverQueryNames) {
    expect(sharedNames.has(name)).toBe(true);
  }
});

test('query name matches const name', () => {
  expect(Object.keys(sharedQueries)).toEqual(Object.keys(mockArgs));
  for (const [key, value] of Object.entries(mockArgs)) {
    const queryFn = sharedQueries[key as keyof typeof sharedQueries];
    const query = (queryFn as NamedQueryImpl)(...value);
    // query names and their exported names should match in zbugs
    expect(query.customQueryID?.name).toBe(key);
    expect(query.customQueryID?.args).toEqual(value);
  }
});

const mockArgs = {
  allLabels: [],
  allUsers: [],
  issuePreload: ['userID'],
  user: ['userID'],
  userPref: ['key', 'userID'],
  userPicker: [false, null, 'creators'],
  issueDetail: ['id', '1', 'userID'],
  prevNext: [null, null, 'next'],
  issueList: [
    {
      open: null,
      assignee: null,
      creator: null,
      labels: null,
      textFilter: null,
      sortField: 'created',
      sortDirection: 'asc',
    },
    'userID',
    10,
  ],
  emojiChange: ['subjectID'],
};
