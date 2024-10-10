import * as esbuild from 'esbuild';
import {expect, test} from 'vitest';
import {replaceNodeUtil} from './replace-node-util.js';

async function compile(contents: string) {
  const res = await esbuild.build({
    stdin: {
      contents,
    },
    write: false,
    plugins: [replaceNodeUtil],
    platform: 'node',
    format: 'esm',
    target: 'esnext',
    bundle: true,
    minify: true,
  });
  return res.outputFiles[0].text;
}

test('Should remove dead code', async () => {
  expect(
    await compile(`import {TextEncoder} from 'util';`),
  ).toMatchInlineSnapshot(`""`);
  expect(
    await compile(`import {TextEncoder} from 'node:util';`),
  ).toMatchInlineSnapshot(`""`);
  expect(await compile(`import * as util from 'util';`)).toMatchInlineSnapshot(
    `""`,
  );
  expect(
    await compile(`import * as util from 'node:util';`),
  ).toMatchInlineSnapshot(`""`);
});

test('Should allow different forms of import', async () => {
  expect(
    await compile(`import {TextEncoder} from 'util';
    console.log(TextEncoder);`),
  ).toMatchInlineSnapshot(`
    "var e=TextEncoder;console.log(e);
    "
  `);
  expect(
    await compile(`import {TextEncoder} from 'node:util';
    console.log(TextEncoder);`),
  ).toMatchInlineSnapshot(`
    "var e=TextEncoder;console.log(e);
    "
  `);
  expect(
    await compile(`import * as mod from 'util';
    console.log(mod.TextEncoder);`),
  ).toMatchInlineSnapshot(`
    "var o=TextEncoder;console.log(o);
    "
  `);
  expect(
    await compile(`import def from 'util';
    console.log(def.TextEncoder);`),
  ).toMatchInlineSnapshot(`
    "var o=TextEncoder,e={TextEncoder:o};console.log(e.TextEncoder);
    "
  `);
});

test('Should allow local reference as well', async () => {
  expect(
    await compile(`import {TextEncoder as UtilTextEncoder} from 'util';
    console.log(UtilTextEncoder);
    console.log(TextEncoder);
    `),
  ).toMatchInlineSnapshot(`
    "var e=TextEncoder;console.log(e);console.log(TextEncoder);
    "
  `);
});
