import {describe, expect, test} from '@jest/globals';
import {defineSecretSafely} from './secrets.js';

describe('secrets', () => {
  test('defineSecretSafely', () => {
    const secret = defineSecretSafely('TEST_SECRET');

    expect(() => secret.value()).toThrowError();

    process.env['TEST_SECRET'] = '';
    expect(secret.value()).toBe('');

    process.env['TEST_SECRET'] = 'foo';
    expect(secret.value()).toBe('foo');
  });
});
