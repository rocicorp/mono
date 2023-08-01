import {expect, test} from '@jest/globals';
import {parseCloudStorageURL} from './cloud-storage.js';

test('parseCloudStorageURL', () => {
  expect(parseCloudStorageURL('gs://bucket-name/filename')).toEqual({
    bucketName: 'bucket-name',
    filename: 'filename',
  });
  expect(parseCloudStorageURL('gcs://bucket-name/filename')).toEqual({
    bucketName: 'bucket-name',
    filename: 'filename',
  });
  expect(parseCloudStorageURL('gs://bucket-name/path/filename')).toEqual({
    bucketName: 'bucket-name',
    filename: 'path/filename',
  });
  expect(parseCloudStorageURL('gcs://bucket-name/path/filename')).toEqual({
    bucketName: 'bucket-name',
    filename: 'path/filename',
  });

  expect(() => parseCloudStorageURL('gs://bucket-name/')).toThrow();
  expect(() => parseCloudStorageURL('https://bucket-name/')).toThrow();
  expect(() => parseCloudStorageURL('gs:///filename')).toThrow();
});
