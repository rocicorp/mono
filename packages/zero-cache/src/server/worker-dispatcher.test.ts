import {expect, test} from 'vitest';
import {parsePath} from './worker-dispatcher.ts';

test.each([
  ['/sync/v1/connect', {version: '1', worker: 'sync', action: 'connect'}],
  ['/sync/v2/connect', {version: '2', worker: 'sync', action: 'connect'}],
  [
    '/sync/v3/connect?foo=bar',
    {version: '3', worker: 'sync', action: 'connect'},
  ],
  [
    '/api/sync/v1/connect',
    {base: 'api', worker: 'sync', version: '1', action: 'connect'},
  ],
  [
    '/api/sync/v1/connect?a=b&c=d',
    {base: 'api', worker: 'sync', version: '1', action: 'connect'},
  ],
  [
    '/zero/sync/v1/connect',
    {base: 'zero', worker: 'sync', version: '1', action: 'connect'},
  ],
  [
    '/zero-api/sync/v0/connect',
    {base: 'zero-api', worker: 'sync', version: '0', action: 'connect'},
  ],
  [
    '/zero-api/sync/v2/connect?',
    {base: 'zero-api', worker: 'sync', version: '2', action: 'connect'},
  ],

  ['/mutate/v1/connect', {version: '1', worker: 'mutate', action: 'connect'}],
  ['/mutate/v2/connect', {version: '2', worker: 'mutate', action: 'connect'}],
  [
    '/mutate/v3/connect?foo=bar',
    {version: '3', worker: 'mutate', action: 'connect'},
  ],
  [
    '/api/mutate/v1/connect',
    {base: 'api', worker: 'mutate', version: '1', action: 'connect'},
  ],
  [
    '/api/mutate/v1/connect?a=b&c=d',
    {base: 'api', worker: 'mutate', version: '1', action: 'connect'},
  ],
  [
    '/zero/mutate/v1/connect',
    {base: 'zero', worker: 'mutate', version: '1', action: 'connect'},
  ],
  [
    '/zero-api/mutate/v0/connect',
    {base: 'zero-api', worker: 'mutate', version: '0', action: 'connect'},
  ],
  [
    '/zero-api/mutate/v2/connect?',
    {base: 'zero-api', worker: 'mutate', version: '2', action: 'connect'},
  ],
  [
    '/replication/v1/changes',
    {version: '1', worker: 'replication', action: 'changes'},
  ],
  [
    '/replication/v2/changes',
    {version: '2', worker: 'replication', action: 'changes'},
  ],
  [
    '/replication/v3/changes?foo=bar',
    {version: '3', worker: 'replication', action: 'changes'},
  ],
  [
    '/api/replication/v1/changes',
    {base: 'api', worker: 'replication', version: '1', action: 'changes'},
  ],
  [
    '/api/replication/v1/changes?a=b&c=d',
    {base: 'api', worker: 'replication', version: '1', action: 'changes'},
  ],

  ['/zero-api/sync/v2/connect/not/match', undefined],
  ['/too/many/components/sync/v0/connect', undefined],
  ['/random/path', undefined],
  ['/', undefined],
  ['', undefined],
])('parseSyncPath %s', (path, result) => {
  expect(parsePath(new URL(path, 'http://foo/'))).toEqual(result);
});
