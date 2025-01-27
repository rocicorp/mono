import {expect, test} from 'vitest';
import {parseSyncPath} from './sync-dispatcher.ts';

test.each([
  ['/sync/v1/connect', {version: '1'}],
  ['/sync/v2/connect', {version: '2'}],
  ['/sync/v3/connect?foo=bar', {version: '3'}],
  ['/api/sync/v1/connect', {base: 'api', version: '1'}],
  ['/api/sync/v1/connect?a=b&c=d', {base: 'api', version: '1'}],
  ['/zero/sync/v1/connect', {base: 'zero', version: '1'}],
  ['/zero-api/sync/v0/connect', {base: 'zero-api', version: '0'}],
  ['/zero-api/sync/v2/connect?', {base: 'zero-api', version: '2'}],

  ['/zero-api/sync/v2/connect/not/match', undefined],
  ['/too/many/components/sync/v0/connect', undefined],
  ['/random/path', undefined],
  ['/', undefined],
  ['', undefined],
])('parseSyncPath %s', (path, result) => {
  expect(parseSyncPath(new URL(path, 'http://foo/'))).toEqual(result);
});
