import {expect, test} from '@jest/globals';
import {userAgentSchema} from 'mirror-protocol/src/user-agent.js';
import * as v from 'shared/src/valita.js';
import {reflectVersionMatcher} from './test-helpers.js';
import {getUserAgent, version} from './version.js';

test('version', () => {
  // We could read the version from package.json, but this way acts as a sanity
  // check.
  expect(version).toBe('0.35.0');
});

test('userAgent', () => {
  expect(getUserAgent()).toMatchObject({
    type: 'reflect-cli',
    version: reflectVersionMatcher,
  });
});

test('userAgent schema', () => {
  v.assert(getUserAgent(), userAgentSchema);
});
