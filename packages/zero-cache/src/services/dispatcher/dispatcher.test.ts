import {expect, test} from 'vitest';
import {parsePath} from './dispatcher.ts';

test.each([
  ['/sync/v1/connect', {version: '1', verb: 'sync'}],
  ['/sync/v2/connect', {version: '2', verb: 'sync'}],
  ['/sync/v3/connect?foo=bar', {version: '3', verb: 'sync'}],
  ['/api/sync/v1/connect', {base: 'api', verb: 'sync', version: '1'}],
  ['/api/sync/v1/connect?a=b&c=d', {base: 'api', verb: 'sync', version: '1'}],
  ['/zero/sync/v1/connect', {base: 'zero', verb: 'sync', version: '1'}],
  ['/zero-api/sync/v0/connect', {base: 'zero-api', verb: 'sync', version: '0'}],
  [
    '/zero-api/sync/v2/connect?',
    {base: 'zero-api', verb: 'sync', version: '2'},
  ],

  ['/mutate/v1/connect', {version: '1', verb: 'mutate'}],
  ['/mutate/v2/connect', {version: '2', verb: 'mutate'}],
  ['/mutate/v3/connect?foo=bar', {version: '3', verb: 'mutate'}],
  ['/api/mutate/v1/connect', {base: 'api', verb: 'mutate', version: '1'}],
  [
    '/api/mutate/v1/connect?a=b&c=d',
    {base: 'api', verb: 'mutate', version: '1'},
  ],
  ['/zero/mutate/v1/connect', {base: 'zero', verb: 'mutate', version: '1'}],
  [
    '/zero-api/mutate/v0/connect',
    {base: 'zero-api', verb: 'mutate', version: '0'},
  ],
  [
    '/zero-api/mutate/v2/connect?',
    {base: 'zero-api', verb: 'mutate', version: '2'},
  ],

  ['/zero-api/sync/v2/connect/not/match', undefined],
  ['/too/many/components/sync/v0/connect', undefined],
  ['/random/path', undefined],
  ['/', undefined],
  ['', undefined],
])('parseSyncPath %s', (path, result) => {
  expect(parsePath(new URL(path, 'http://foo/'))).toEqual(result);
});
