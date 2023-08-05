import {expect, jest, test} from '@jest/globals';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import {fileURLToPath} from 'node:url';
import {publishHandler, type PublishCaller} from './publish.js';
import {useFakeAppConfig, useFakeAuthConfig} from './test-helpers.js';
import type {Firestore} from './firebase.js';

type Args = Parameters<typeof publishHandler>[0];

useFakeAuthConfig();
useFakeAppConfig();

test('it should throw if file not found', async () => {
  const script = `./test${Math.random().toString(32).slice(2)}.ts`;

  await expect(publishHandler({script} as Args)).rejects.toEqual(
    expect.objectContaining({
      constructor: Error,
      message: expect.stringMatching(
        new RegExp('^File not found: .*' + script + '$'),
      ),
    }),
  );
});

async function writeTempFiles(
  data: string,
  filename = 'test.js',
  reflectVersion?: string,
) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'reflect-publish-test-'));
  const testFilePath = path.join(dir, filename);
  await fs.writeFile(testFilePath, data, 'utf-8');

  if (!reflectVersion) {
    const currentFile = fileURLToPath(import.meta.url);
    const reflectPackageJSONPath = path.join(
      path.dirname(currentFile), // src
      '..', // reflect-cli
      '..', // mirror
      '..', // mono
      'packages',
      'reflect',
      'package.json',
    );
    const s = await fs.readFile(reflectPackageJSONPath, 'utf-8');
    const reflectPackageJSON = JSON.parse(s);
    reflectVersion = '>=' + reflectPackageJSON.version;
  }

  const packageJSONFilePath = path.join(dir, 'package.json');
  await fs.writeFile(
    packageJSONFilePath,
    JSON.stringify({
      dependencies: {
        '@rocicorp/reflect': reflectVersion,
      },
    }),
    'utf-8',
  );
  return testFilePath;
}

test('it should throw if the source has syntax errors', async () => {
  const testFilePath = await writeTempFiles('const x =');
  await expect(publishHandler({script: testFilePath} as Args)).rejects.toEqual(
    expect.objectContaining({
      constructor: Error,
      message: expect.stringMatching(/Unexpected end of file/),
    }),
  );
});

test('it should compile typescript', async () => {
  const publishMock = jest.fn();
  publishMock.mockImplementationOnce(body => {
    expect(body).toMatchObject({
      requester: {
        userAgent: {
          type: 'reflect-cli',
          version: '0.1.0',
        },
        userID: 'fake-uid',
      },
      source: {
        content: expect.stringContaining(`var x = 42;`),
        name: 'test.js',
      },
      sourcemap: {content: expect.any(String), name: 'test.js.map'},
    });
    return Promise.resolve({
      success: true,
      deploymentPath: 'apps/foo/deployments/bar',
    });
  });

  const testFilePath = await writeTempFiles(
    'const x: number = 42; console.log(x);',
    'test.ts',
  );

  // 'firestore-jest-mocks doesn't implement the same onSnapshot() API
  // as that of the client JS sdk. Just fill in enough to get the test to pass.
  const fakeFirestore = {
    doc: (path: string) => {
      expect(path).toBe('apps/foo/deployments/bar');
      return {
        withConverter: () => ({
          onSnapshot: (callbacks: {error: (e: unknown) => void}) => {
            callbacks.error(
              new Error('unimplemented. just getting the test to pass'),
            );
            return () => {
              /* do nothing */
            };
          },
        }),
      };
    },
  } as unknown as Firestore;

  await publishHandler(
    {script: testFilePath} as Args,
    undefined,
    publishMock as unknown as PublishCaller,
    fakeFirestore,
  );

  expect(publishMock).toHaveBeenCalledTimes(1);
});

test('it should throw if invalid version', async () => {
  const testFilePath = await writeTempFiles(
    'const x = 42;',
    'test.ts',
    '1.0.0',
  );
  await expect(publishHandler({script: testFilePath} as Args)).rejects.toEqual(
    expect.objectContaining({
      constructor: Error,
      message: expect.stringMatching(
        /^Unsupported version range "1.0.0" for "@rocicorp\/reflect" in /,
      ),
    }),
  );
});
