import type {Bucket} from '@google-cloud/storage';
import {expect, test} from '@jest/globals';
import {sha256OfString, storeModule} from './store-module.js';

test('basic', async () => {
  expect(await sha256OfString('foo')).toBe(
    '2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae',
  );
});

test('storeBucket', async () => {
  let saveCalls = 0;
  const module = {name: 'test/index.js', content: 'console.log("hello")'};
  const file = {
    cloudStorageURI: {href: ''},
    metadata: {},
    save(content: string) {
      saveCalls++;
      expect(content).toBe(module.content);
      expect(this.metadata).toEqual({moduleName: 'test/index.js'});
      this.cloudStorageURI = {href: 'gs://dummy'};
      return Promise.resolve();
    },
    exists() {
      return Promise.resolve([false]);
    },
  };
  const bucket = {
    file(filename: string) {
      expect(filename).toBe(
        '425de7eed0bfd83eb049395063c45c38a5e1ab4db37dd692ef88e869bdb616c',
      );
      return file;
    },
  };
  expect(await storeModule(bucket as unknown as Bucket, module)).toBe(
    'gs://dummy',
  );

  expect(saveCalls).toBe(1);

  // Writing again, exists is true.
  file.exists = () => Promise.resolve([true]);
  expect(await storeModule(bucket as unknown as Bucket, module)).toBe(
    'gs://dummy',
  );
  expect(saveCalls).toBe(1);
});
