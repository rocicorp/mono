import type {Bucket} from '@google-cloud/storage';
import {nanoid} from 'nanoid';

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
  const filename = `${encodeURIComponent(module.name)}-${nanoid()}`;
  const file = bucket.file(filename);
  await file.save(module.content);
  return file.cloudStorageURI.href;
}
