import assert from 'node:assert';

export function parseCloudStorageURL(url: string): {
  bucketName: string;
  filename: string;
} {
  // Don't use the built in URL class, because Node.js implementation is not
  // standards compliant.
  const m = url.match(/^gc?s:\/\/([^/]+)\/(.+)$/);
  assert(m);
  return {bucketName: m[1], filename: m[2]};
}
