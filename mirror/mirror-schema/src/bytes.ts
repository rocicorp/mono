import * as v from 'shared/src/valita.js';

// Subset of Bytes type returned in the client-side Firestore sdk:
// https://firebase.google.com/docs/reference/js/firestore_.bytes
interface Bytes {
  toUint8Array(): Uint8Array;
}

// Schema suitable for parsing a field of type Bytes (https://firebase.google.com/docs/firestore/manage-data/data-types)
// from either the server or client SDKs. The former uses a Uint8Array to represent
// the data, which is used directly. The client SDK, on the other hand, uses a custom "Bytes"
// object, which the schema parser normalizes into a Uint8Array.
export const bytesSchema = v.unknown().chain(val => {
  if (val instanceof Uint8Array) {
    // Returned by server SDK
    return v.ok(val);
  }
  if (typeof (val as Bytes).toUint8Array === 'function') {
    // Returned by client SDK
    const arr = (val as Bytes).toUint8Array();
    if (arr instanceof Uint8Array) {
      return v.ok(arr);
    }
  }
  return v.err(`Expected Uint8Array or Bytes but got ${String(val)}`);
});

export const keySpecSchema = v.object({
  // The name of the Secret Manager secret in which the key is stored,
  // or absent for the default secret (which may be context dependent).
  secretName: v.string().optional(),
  // The version of the secret.
  version: v.number(),
});

export type KeySpec = v.Infer<typeof keySpecSchema>;

// Use the same cipher algorithm that Google uses for encryption at rest:
// https://cloud.google.com/docs/security/encryption/default-encryption
export const DEFAULT_CIPHER_ALGORITHM = 'aes-256-cbc';

export const encryptedBytesSchema = v.object({
  algo: v.string().optional(), // or DEFAULT_CIPHER_ALGORITHM
  key: keySpecSchema,
  iv: bytesSchema,
  bytes: bytesSchema,
});

export type EncryptedBytes = v.Infer<typeof encryptedBytesSchema>;
