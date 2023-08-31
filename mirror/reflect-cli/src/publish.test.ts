import {expect, jest, test, beforeAll, afterEach} from '@jest/globals';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import {fileURLToPath} from 'node:url';
import {publishHandler, type PublishCaller} from './publish.js';
import {useFakeAuthConfig} from './test-helpers.js';
import {
  deploymentDataConverter,
  defaultOptions,
} from 'mirror-schema/src/deployment.js';
import {Timestamp} from '@google-cloud/firestore';
import {fakeFirestore} from 'mirror-schema/src/test-helpers.js';
import {initFirebase} from './firebase.js';
import {setAppConfigForTesting} from './app-config.js';

type Args = Parameters<typeof publishHandler>[0];

useFakeAuthConfig();

beforeAll(() => {
  initFirebase('local');
});

afterEach(() => {
  setAppConfigForTesting(undefined);
});

test('it should throw if file not found', async () => {
  const script = `./test${Math.random().toString(32).slice(2)}.ts`;
  setAppConfigForTesting({
    apps: {default: {appID: 'test-app-id'}},
    server: script,
  });

  await expect(publishHandler({} as Args)).rejects.toEqual(
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
  setAppConfigForTesting({
    apps: {default: {appID: 'test-app-id'}},
    server: testFilePath,
  });

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
}

test('it should throw if the source has syntax errors', async () => {
  await writeTempFiles('const x =');
  await expect(publishHandler({} as Args)).rejects.toEqual(
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
          version: '0.31.0',
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

  await writeTempFiles('const x: number = 42; console.log(x);', 'test.ts');

  // Set the Deployment doc to RUNNING so that the cli command exits.
  const firestore = fakeFirestore();
  await firestore
    .doc('apps/foo/deployments/bar')
    .withConverter(deploymentDataConverter)
    .set({
      deploymentID: 'bar',
      requesterID: 'foo',
      type: 'USER_UPLOAD',
      status: 'RUNNING',
      spec: {
        appModules: [],
        hostname: 'app-name.reflect-server-net',
        serverVersion: '0.1.0',
        serverVersionRange: '^0.31.0',
        options: defaultOptions(),
        hashesOfSecrets: {
          /* eslint-disable @typescript-eslint/naming-convention */
          REFLECT_AUTH_API_KEY: 'aaa',
          DATADOG_LOGS_API_KEY: 'bbb',
          DATADOG_METRICS_API_KEY: 'ccc',
          /* eslint-enable @typescript-eslint/naming-convention */
        },
      },
      requestTime: Timestamp.now(),
    });

  await publishHandler(
    {} as Args,
    publishMock as unknown as PublishCaller,
    firestore,
  );

  expect(publishMock).toHaveBeenCalledTimes(1);
});

test('it should throw if invalid version', async () => {
  await writeTempFiles('const x = 42;', 'test.ts', '1.0.0');
  await expect(publishHandler({} as Args)).rejects.toEqual(
    expect.objectContaining({
      constructor: Error,
      message: expect.stringMatching(
        /^Unsupported version range "1.0.0" for "@rocicorp\/reflect" in /,
      ),
    }),
  );
});
