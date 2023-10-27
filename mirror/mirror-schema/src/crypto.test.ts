import {expect, test} from '@jest/globals';
import crypto from 'node:crypto';
import {decrypt, encrypt} from './crypto.js';

test('encryption / decryption', () => {
  const key = crypto.randomBytes(32);
  const keySpec = {version: 2};

  const encrypted1 = encrypt(
    Buffer.from('this is the plaintext', 'utf-8'),
    key,
    keySpec,
  );

  expect(encrypted1.key).toEqual(keySpec);
  expect(encrypted1.iv.length).toBe(16);
  expect(encrypted1.bytes.length).toBe(32); // Two cipher blocks
  expect(Buffer.from(encrypted1.bytes).toString('utf-8')).not.toBe(
    'this is the plaintext',
  );

  const encrypted2 = encrypt(
    Buffer.from('this is the plaintext', 'utf-8'),
    key,
    keySpec,
  );
  expect(encrypted2.key).toEqual(keySpec);
  expect(encrypted2.iv.length).toBe(16);
  expect(encrypted2.bytes.length).toBe(32); // Two cipher blocks
  expect(Buffer.from(encrypted2.bytes).toString('utf-8')).not.toBe(
    'this is the plaintext',
  );

  // Encryptions of the same plaintext should result in different
  // IVs and thus different ciphertexts.
  expect(Buffer.from(encrypted1.bytes).toString('hex')).not.toEqual(
    Buffer.from(encrypted2.bytes).toString('hex'),
  );

  const decrypted1 = decrypt(encrypted1, key);
  expect(Buffer.from(decrypted1).toString('utf-8')).toBe(
    'this is the plaintext',
  );

  const decrypted2 = decrypt(encrypted2, key);
  expect(Buffer.from(decrypted2).toString('utf-8')).toBe(
    'this is the plaintext',
  );
});
