import {expect, test} from '@jest/globals';
import {userAgentSchema} from 'mirror-protocol/src/user-agent.js';
import * as v from 'shared/src/valita.js';
import {userAgent, version} from './version.js';

test('version', () => {
  expect(version).toBe('0.1.2');
});

test('userAgent', () => {
  expect(userAgent).toMatchInlineSnapshot(`
    {
      "type": "reflect-cli",
      "version": "0.1.2",
    }
  `);
});

test('userAgent schema', () => {
  v.assert(userAgent, userAgentSchema);
});
