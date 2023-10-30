import crypto from 'node:crypto';
import {
  DEFAULT_CIPHER_ALGORITHM,
  type EncryptedBytes,
  type KeySpec,
} from './bytes.js';

export function encryptUtf8(
  plaintext: string,
  key: Uint8Array,
  keySpec: KeySpec,
  ivOrLen: Uint8Array | number = 16,
  algo?: string,
): EncryptedBytes {
  const iv =
    typeof ivOrLen === 'number' ? crypto.randomBytes(ivOrLen) : ivOrLen;
  const cipher = crypto.createCipheriv(
    algo ?? DEFAULT_CIPHER_ALGORITHM,
    key,
    iv,
  );
  const ciphertext = Buffer.concat([
    cipher.update(plaintext, 'utf-8'),
    cipher.final(),
  ]);
  const encryptedBytes: EncryptedBytes = {
    key: keySpec,
    iv,
    ciphertext,
  };
  if (algo) {
    encryptedBytes.algo = algo;
  }
  return encryptedBytes;
}

export function decryptUtf8(
  encryptedBytes: EncryptedBytes,
  key: Uint8Array,
): string {
  const {algo, iv, ciphertext} = encryptedBytes;
  const decipher = crypto.createDecipheriv(
    algo ?? DEFAULT_CIPHER_ALGORITHM,
    key,
    iv,
  );
  return Buffer.concat([
    decipher.update(ciphertext),
    decipher.final(),
  ]).toString('utf-8');
}
