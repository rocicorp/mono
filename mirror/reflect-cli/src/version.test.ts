import {expect, test} from '@jest/globals';
import {userAgentSchema} from 'mirror-protocol/src/user-agent.js';
import * as v from 'shared/src/valita.js';
import reflectPackageJSON from '../../../packages/reflect/package.json';
import {reflectVersionMatcher} from './test-helpers.js';
import {getUserAgent, version} from './version.js';

test('version', () => {
  expect(version).toBe(reflectPackageJSON.version);
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
