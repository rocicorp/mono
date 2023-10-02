import {expect, test} from '@jest/globals';
import {userAgentSchema} from 'mirror-protocol/src/user-agent.js';
import * as v from 'shared/src/valita.js';
import {getUserAgent, version} from './version.js';
import {reflectVersionMatcher} from './test-helpers.js';

test('version', () => {
  expect(version).toBe('0.1.2');
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
