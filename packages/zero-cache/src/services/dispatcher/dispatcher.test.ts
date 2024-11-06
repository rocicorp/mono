import {expect, test} from 'vitest';
import {parseSyncPath} from './dispatcher.js';

test.each([
  ['/api/sync/v1/connect', {base: 'api', version: 'v1'}],
  ['/api/sync/v1/connect?a=b&c=d', {base: 'api', version: 'v1'}],
  ['/zero/sync/v1/connect', {base: 'zero', version: 'v1'}],
  ['/zero-api/sync/v0/connect', {base: 'zero-api', version: 'v0'}],
  ['/zero-api/sync/v2/connect?', {base: 'zero-api', version: 'v2'}],
  ['/zero-api/sync/v2/connect/not/match', undefined],
  ['/not/valid/sync/v0/connect', undefined],
  ['/random/path', undefined],
  ['/', undefined],
  ['', undefined],
])('parseSyncPath %s', (path, result) => {
  expect(parseSyncPath(new URL(path, 'http://foo/'))).toEqual(result);
});
