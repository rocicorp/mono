import type {Bucket} from '@google-cloud/storage';
import type {Storage} from 'firebase-admin/storage';
import {expect, test} from '@jest/globals';
import {loadModule, sha256OfString, storeModule} from './module.js';
import type {Module} from './module.js';

test('basic', async () => {
  expect(await sha256OfString('foo')).toBe(
    '2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae',
  );
});

test('storeModule', async () => {
  let saveCalls = 0;
  const module: Module = {
    name: 'test/index.js',
    content: 'console.log("hello")',
    type: 'esm',
  };
  const file = {
    cloudStorageURI: {href: ''},
    metadata: {},
    save(content: string) {
      saveCalls++;
      expect(content).toBe(module.content);
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
  expect(await storeModule(bucket as unknown as Bucket, module)).toEqual({
    name: 'test/index.js',
    type: 'esm',
    url: 'gs://dummy',
  });

  expect(saveCalls).toBe(1);

  // Writing again, exists is true.
  file.exists = () => Promise.resolve([true]);
  expect(await storeModule(bucket as unknown as Bucket, module)).toEqual({
    name: 'test/index.js',
    type: 'esm',
    url: 'gs://dummy',
  });
  expect(saveCalls).toBe(1);
});

test('loadModule', async () => {
  type Case = {
    url: string;
    expectedBucket: string;
    expectedFile: string;
  };
  const cases: Case[] = [
    {
      url: 'gs://mah-bucket/425de7eed0bfd83eb049395063c45c38a5e1ab4db37dd692ef88e869bdb616c',
      expectedBucket: 'mah-bucket',
      expectedFile:
        '425de7eed0bfd83eb049395063c45c38a5e1ab4db37dd692ef88e869bdb616c',
    },
    {
      url: 'gs://another-bucket/524de7eed0bfd83eb049395063c45c38a5e1ab4db37dd692ef88e869bdb616c',
      expectedBucket: 'another-bucket',
      expectedFile:
        '524de7eed0bfd83eb049395063c45c38a5e1ab4db37dd692ef88e869bdb616c',
    },
  ];

  for (const c of cases) {
    const file = {
      download: () => [Buffer.from('module-contents 🌊', 'utf-8')],
    };
    const bucket = {
      file: (filename: string) => {
        expect(filename).toBe(c.expectedFile);
        return {get: () => Promise.resolve([file])};
      },
    };
    const storage = {
      bucket: (bucketName: string) => {
        expect(bucketName).toBe(c.expectedBucket);
        return bucket;
      },
    } as unknown as Storage;

    expect(
      await loadModule(storage, {
        name: 'foo.js',
        url: c.url,
        type: 'esm',
      }),
    ).toEqual({
      name: 'foo.js',
      type: 'esm',
      content: 'module-contents 🌊',
    });
  }
});
