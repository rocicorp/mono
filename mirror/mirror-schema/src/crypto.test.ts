import {expect, test} from '@jest/globals';
import crypto from 'node:crypto';
import {decrypt, encrypt} from './crypto.js';

test('encryption / decryption', () => {
  const key = crypto.randomBytes(32);
  const keySpec = {version: 2};

  const encrypted = encrypt(
    Buffer.from('this is the plaintext', 'utf-8'),
    key,
    keySpec,
  );

  expect(encrypted.key).toEqual(keySpec);
  expect(encrypted.iv.length).toBe(16);
  expect(encrypted.bytes.length).toBe(32); // Two cipher blocks
  expect(Buffer.from(encrypted.bytes).toString('utf-8')).not.toBe(
    'this is the plaintext',
  );

  const decrypted = decrypt(encrypted, key);
  expect(Buffer.from(decrypted).toString('utf-8')).toBe(
    'this is the plaintext',
  );
});
