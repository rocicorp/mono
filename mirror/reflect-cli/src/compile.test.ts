import {expect, test} from '@jest/globals';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import {compile} from './compile.js';

async function writeTempFile(data: string, filename = 'test.js') {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'reflect-compile-test-'));
  const testFilePath = path.join(dir, filename);
  await fs.writeFile(testFilePath, data, 'utf-8');
  return testFilePath;
}

test('it should throw if the source has syntax errors', async () => {
  const testFilePath = await writeTempFile('const x =');
  await expect(compile(testFilePath, true)).rejects.toEqual(
    expect.objectContaining({
      constructor: Error,
      message: expect.stringMatching(/Unexpected end of file/),
    }),
  );
});

function stripCommentLines(str: string) {
  return str
    .split('\n')
    .filter(line => line.trim() !== '')
    .filter(line => !line.trimStart().startsWith('//'))
    .join('\n')
    .trim();
}

test('it should compile typescript', async () => {
  const testFilePath = await writeTempFile(
    'const x: number = 42; console.log(x);',
    'test.ts',
  );

  const result = await compile(testFilePath, true);
  expect(result.code.path).toBe(path.resolve('test.js'));
  expect(result.sourcemap.path).toBe(path.resolve('test.js.map'));

  expect(stripCommentLines(result.code.text)).toMatchInlineSnapshot(`
    "var x = 42;
    console.log(x);"
  `);
});

test('it should replace @replicache/reflect/server', async () => {
  const testFilePath = await writeTempFile(
    `import * as reflectServer from '@rocicorp/reflect/server';
console.log(reflectServer);`,
    'test.ts',
  );

  const result = await compile(testFilePath, true);
  expect(result.code.path).toBe(path.resolve('test.js'));
  expect(result.sourcemap.path).toBe(path.resolve('test.js.map'));

  expect(stripCommentLines(result.code.text)).toMatchInlineSnapshot(`
    "import * as reflectServer from "./reflect-server.js";
    console.log(reflectServer);"
  `);
});

test('it should bundle into one file', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'reflect-compile-test-'));
  const fileA = path.join(dir, 'a.js');
  await fs.writeFile(
    fileA,
    `import {b} from './b.js'; console.log(b);`,
    'utf-8',
  );
  const fileB = path.join(dir, 'b.js');
  await fs.writeFile(fileB, `export const b = 'BBB';`, 'utf-8');

  const result = await compile(fileA, true);
  expect(result.code.path).toBe(path.resolve('a.js'));
  expect(result.sourcemap.path).toBe(path.resolve('a.js.map'));

  expect(stripCommentLines(result.code.text)).toMatchInlineSnapshot(`
    "var b = "BBB";
    console.log(b);"
  `);
});
