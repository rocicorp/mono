import {expect, test} from '@jest/globals';
import crypto from 'node:crypto';
import {decryptUtf8, encryptUtf8} from './crypto.js';

test('encryption / decryption', () => {
  const key = crypto.randomBytes(32);
  const keySpec = {version: '2'};

  const encrypted1 = encryptUtf8('this is the plaintext', key, keySpec);

  expect(encrypted1.key).toEqual(keySpec);
  expect(encrypted1.iv.length).toBe(16);
  expect(encrypted1.ciphertext.length).toBe(32); // Two cipher blocks
  expect(Buffer.from(encrypted1.ciphertext).toString('utf-8')).not.toBe(
    'this is the plaintext',
  );

  const encrypted2 = encryptUtf8('this is the plaintext', key, keySpec);
  expect(encrypted2.key).toEqual(keySpec);
  expect(encrypted2.iv.length).toBe(16);
  expect(encrypted2.ciphertext.length).toBe(32); // Two cipher blocks
  expect(Buffer.from(encrypted2.ciphertext).toString('utf-8')).not.toBe(
    'this is the plaintext',
  );

  // Encryptions of the same plaintext should result in different
  // IVs and thus different ciphertexts.
  expect(Buffer.from(encrypted1.ciphertext).toString('hex')).not.toEqual(
    Buffer.from(encrypted2.ciphertext).toString('hex'),
  );

  const decrypted1 = decryptUtf8(encrypted1, key);
  expect(decrypted1).toBe('this is the plaintext');

  const decrypted2 = decryptUtf8(encrypted2, key);
  expect(decrypted2).toBe('this is the plaintext');
});
