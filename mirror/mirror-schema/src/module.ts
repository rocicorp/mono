import type {Bucket} from '@google-cloud/storage';
import * as crypto from 'shared/src/mirror/crypto.js';

type Module = {
  name: string;
  content: string;
};

/**
 * Stores the module in Google Cloud Storage and returns the URL (gs://...) of
 * the uploaded file.
 */
export async function storeModule(
  bucket: Bucket,
  module: Module,
): Promise<string> {
  const filename = await sha256OfString(module.content);
  const file = bucket.file(filename);
  const [exists] = await file.exists();
  if (exists) {
    return file.cloudStorageURI.href;
  }
  file.metadata = {moduleName: module.name};
  await file.save(module.content);
  return file.cloudStorageURI.href;
}

export async function sha256OfString(s: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(s);
  const hash = await crypto.subtle.digest('SHA-256', data);
  return hexStringFromBuffer(hash);
}

function hexStringFromBuffer(hash: ArrayBuffer): string {
  let s = '';
  for (const byte of new Uint8Array(hash)) {
    s += byte < 10 ? '0' : '' + byte.toString(16);
  }
  return s;
}
