import crypto from 'node:crypto';
import {
  DEFAULT_CIPHER_ALGORITHM,
  type EncryptedBytes,
  type KeySpec,
} from './bytes.js';

export function encrypt(
  plaintext: Uint8Array,
  key: Uint8Array,
  keySpec: KeySpec,
  algo?: string,
  ivLen = 16,
): EncryptedBytes {
  const iv = crypto.randomBytes(ivLen);
  const cipher = crypto.createCipheriv(
    algo ?? DEFAULT_CIPHER_ALGORITHM,
    key,
    iv,
  );
  const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
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

export function decrypt(
  encryptedBytes: EncryptedBytes,
  key: Uint8Array,
): Uint8Array {
  const {algo, iv, ciphertext} = encryptedBytes;
  const decipher = crypto.createDecipheriv(
    algo ?? DEFAULT_CIPHER_ALGORITHM,
    key,
    iv,
  );
  return Buffer.concat([decipher.update(ciphertext), decipher.final()]);
}
