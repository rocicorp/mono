import {beforeEach, describe, expect, test} from '@jest/globals';
import type {EncryptedBytes} from 'mirror-schema/src/bytes.js';
import {encryptUtf8} from 'mirror-schema/src/crypto.js';
import {ENCRYPTION_KEY_SECRET_NAME} from 'mirror-schema/src/env.js';
import {TestSecrets} from '../../secrets/test-utils.js';
import {
  LEGACY_REFLECT_API_KEY,
  REFLECT_API_KEY,
  decryptSecrets,
  defineSecretSafely,
} from './secrets.js';

describe('secrets', () => {
  test('defineSecretSafely', () => {
    const secret = defineSecretSafely('TEST_SECRET');

    expect(() => secret.value()).toThrowError();

    process.env['TEST_SECRET'] = '';
    expect(secret.value()).toBe('');

    process.env['TEST_SECRET'] = 'foo';
    expect(secret.value()).toBe('foo');
  });

  describe('decryptSecrets', () => {
    let secrets: TestSecrets;
    let encryptedApiKey: EncryptedBytes;

    beforeEach(() => {
      secrets = new TestSecrets([
        ENCRYPTION_KEY_SECRET_NAME,
        '2',
        TestSecrets.TEST_KEY,
      ]);
      encryptedApiKey = encryptUtf8(
        'the-bestest-api-key-ever',
        Buffer.from(TestSecrets.TEST_KEY, 'base64url'),
        {version: '2'},
      );
    });

    test('with new api key', async () => {
      const decrypted = await decryptSecrets(secrets, {
        [REFLECT_API_KEY]: encryptedApiKey,
      });
      expect(decrypted).toEqual({
        [REFLECT_API_KEY]: 'the-bestest-api-key-ever',
        [LEGACY_REFLECT_API_KEY]: 'the-bestest-api-key-ever',
      });
    });

    test('with legacy api key', async () => {
      const decrypted = await decryptSecrets(secrets, {
        [LEGACY_REFLECT_API_KEY]: encryptedApiKey,
      });
      expect(decrypted).toEqual({
        [REFLECT_API_KEY]: 'the-bestest-api-key-ever',
        [LEGACY_REFLECT_API_KEY]: 'the-bestest-api-key-ever',
      });
    });
  });
});
