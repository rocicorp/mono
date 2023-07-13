import {expect, jest, test} from '@jest/globals';
import * as fs from 'node:fs/promises';
import {createRequire} from 'node:module';
import * as os from 'node:os';
import * as path from 'node:path';
import {pkgUp} from 'pkg-up';
import {assert} from 'shared/src/asserts.js';
import {publishHandler} from './publish.js';
import {useFakeAuthConfig} from './test-helpers.js';

type Args = Parameters<typeof publishHandler>[0];

useFakeAuthConfig();

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
    const require = createRequire(import.meta.url);
    const reflectPath = require.resolve('@rocicorp/reflect');
    const reflectPackageJSONPath = await pkgUp({cwd: reflectPath});
    assert(reflectPackageJSONPath);
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
      name: 'test-name',
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
    return Promise.resolve(new Response('{"result":{"success":"OK"}}'));
  });

  const testFilePath = await writeTempFiles(
    'const x: number = 42; console.log(x);',
    'test.ts',
  );
  await publishHandler(
    {script: testFilePath, name: 'test-name'} as Args,
    publishMock as any,
  );

  expect(publishMock).toHaveBeenCalledTimes(1);
});

test('it should throw if invalid version', async () => {
  const testFilePath = await writeTempFiles(
    'const x = 42;',
    'test.ts',
    '1.0.0',
  );
  await expect(
    publishHandler({script: testFilePath, name: 'test-name'} as Args),
  ).rejects.toEqual(
    expect.objectContaining({
      constructor: Error,
      message: expect.stringMatching(
        /^Unsupported version range "1.0.0" for "@rocicorp\/reflect" in /,
      ),
    }),
  );
});
