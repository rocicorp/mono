import {describe, expect, test} from '@jest/globals';
import {fail} from 'node:assert';
import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';
import type {AbortError} from 'shared/src/abort-error.js';
import {Queue} from 'shared/src/queue.js';
import {sleep} from 'shared/src/sleep.js';
import {CompileResult, compile, watch} from './compile.js';

async function writeTempFile(data: string, filename = 'test.js') {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'reflect-compile-test-'));
  const testFilePath = path.join(dir, filename);
  await fs.writeFile(testFilePath, data, 'utf-8');
  return testFilePath;
}

test('it should throw if the source has syntax errors', async () => {
  const testFilePath = await writeTempFile('const x =');
  await expect(compile(testFilePath, true, 'production')).rejects.toEqual(
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

  const result = await compile(testFilePath, true, 'production');
  expect(result.code.path).toBe(path.resolve('test.js'));
  expect(result.sourcemap?.path).toBe(path.resolve('test.js.map'));

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

  const result = await compile(testFilePath, true, 'production');
  expect(result.code.path).toBe(path.resolve('test.js'));
  expect(result.sourcemap?.path).toBe(path.resolve('test.js.map'));

  expect(stripCommentLines(result.code.text)).toMatchInlineSnapshot(`
    "import * as reflectServer from "./reflect-server.js";
    console.log(reflectServer);"
  `);
});

describe('it should replace process.env.NODE_ENV', () => {
  for (const mode of ['production', 'development'] as const) {
    test(mode, async () => {
      const testFilePath = await writeTempFile(
        `console.log(process.env.NODE_ENV);`,
        'test.ts',
      );

      const result = await compile(testFilePath, true, mode);
      expect(result.code.path).toBe(path.resolve('test.js'));
      expect(result.sourcemap?.path).toBe(path.resolve('test.js.map'));

      expect(stripCommentLines(result.code.text)).toBe(
        `console.log("${mode}");`,
      );
    });
  }
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

  const result = await compile(fileA, true, 'production');
  expect(result.code.path).toBe(path.resolve('a.js'));
  expect(result.sourcemap?.path).toBe(path.resolve('a.js.map'));

  expect(stripCommentLines(result.code.text)).toMatchInlineSnapshot(`
    "var b = "BBB";
    console.log(b);"
  `);
});

describe('watch', () => {
  type CheckResult = (snapshot: string) => Promise<void>;
  type Write = (
    filename: string,
    data: string,
    options: {compilationExpected: false} | {expectedResult: string},
  ) => Promise<void>;

  async function watchHarness(
    entryPoint: string,
    testBody: (checkResult: CheckResult, write: Write) => Promise<void>,
  ) {
    const ac = new AbortController();
    const q = new Queue<CompileResult>();

    let compilationExpected = true;

    const write: Write = async (filename, data, options) => {
      compilationExpected = !('compilationExpected' in options);
      await sleep(50);
      await fs.writeFile(filename, data, 'utf-8');
      if ('expectedResult' in options) {
        await checkResult(options.expectedResult);
      }
    };

    const checkResult: CheckResult = async (snapshot: string) => {
      const result = await q.dequeue();
      expect(result.code.path).toBe(path.resolve('a.js'));
      expect(result.sourcemap?.path).toBe(path.resolve('a.js.map'));
      expect(stripCommentLines(result.code.text)).toBe(snapshot);
    };

    try {
      (async () => {
        try {
          for await (const change of watch(
            entryPoint,
            true,
            'development',
            ac.signal,
          )) {
            if (!compilationExpected) {
              throw new Error('Unexpected recompilation');
            }
            q.enqueue(change).catch(e => fail(e));
          }
        } catch (e) {
          // In Jest e is not an instance of Error?!?
          // https://github.com/jestjs/jest/issues/2549
          if ((e as AbortError).name !== 'AbortError') {
            throw e;
          }
        }
      })().catch(e => fail(e));

      await testBody(checkResult, write);

      await sleep(100);
    } finally {
      ac.abort();
    }
  }

  test('watch should work', async () => {
    const dir = await fs.mkdtemp(
      path.join(os.tmpdir(), 'reflect-compile-test-'),
    );
    const fileA = path.join(dir, 'a.js');
    await fs.writeFile(
      fileA,
      `import {b} from './b.js'; console.log(b);`,
      'utf-8',
    );
    const fileB = path.join(dir, 'b.js');
    await fs.writeFile(fileB, `export const b = 'BBB';`, 'utf-8');

    await watchHarness(fileA, async (checkResult, write) => {
      await checkResult(`var b = "BBB";
console.log(b);`);

      await write(fileA, `console.log('changed');`, {
        expectedResult: `console.log("changed");`,
      });

      // Changing b now should not trigger the watcher since a no longer depends on b.
      await write(fileB, `console.log('changed b');`, {
        compilationExpected: false,
      });

      await write(fileA, `console.log('changed a again');`, {
        expectedResult: `console.log("changed a again");`,
      });

      await write(fileA, `console.log(process.env.NODE_ENV);`, {
        expectedResult: `console.log("development");`,
      });
    });
  });

  test('watch should continue after syntax errors', async () => {
    const dir = await fs.mkdtemp(
      path.join(os.tmpdir(), 'reflect-compile-test-'),
    );
    const fileA = path.join(dir, 'a.js');
    await fs.writeFile(fileA, `console.log(1);`, 'utf-8');

    await watchHarness(fileA, async (checkResult, write) => {
      await checkResult(`console.log(1);`);

      await write(fileA, `console.log(`, {compilationExpected: false});

      await write(fileA, `console.log('`, {compilationExpected: false});

      await write(fileA, `console.log(2);`, {
        expectedResult: `console.log(2);`,
      });
    });
  });
});
