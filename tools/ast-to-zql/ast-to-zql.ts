/* eslint-disable no-console */
import {readFile} from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import {createInterface} from 'node:readline';
import {format, resolveConfig} from 'prettier';
import {astToZQL} from '../../packages/zql/src/query/ast-to-zql.ts';

function isStdinPiped(): boolean {
  return !process.stdin.isTTY;
}

function readFromStdin(): Promise<string> {
  return new Promise(resolve => {
    let data = '';
    const rl = createInterface({
      input: process.stdin,
      output: process.stdout,
      terminal: false,
    });

    rl.on('line', line => {
      data += line + '\n';
    });

    rl.on('close', () => {
      resolve(data);
    });
  });
}

async function formatOutput(content: string): Promise<string> {
  try {
    const options = (await resolveConfig(new URL(import.meta.url))) ?? {};
    return await format(content, {
      ...options,
      parser: 'typescript',
      semi: false,
    });
  } catch (error) {
    console.warn('Warning: Unable to format output with prettier:', error);
    return content;
  }
}

async function main(): Promise<void> {
  try {
    let input: string;

    if (isStdinPiped()) {
      input = await readFromStdin();
    } else if (process.argv.length > 2) {
      const filePath = process.argv[2];
      input = await readFile(filePath, 'utf-8');
    } else {
      const execName = path.basename(process.argv[1]);
      console.error('Error: No input provided.');
      console.error('Usage:');
      const runtime = process.versions.bun
        ? 'bun'
        : process.versions.deno
          ? 'deno'
          : 'node';
      console.error(`  cat ast.json | ${runtime} ${execName}`);
      console.error(`  ${runtime} ${execName} ast.json`);
      process.exit(1);
    }

    const ast = JSON.parse(input);

    const zql = astToZQL(ast);
    const code = `query.${ast.table}${zql}`;

    console.log(await formatOutput(code));
  } catch (error) {
    console.error('Error processing input:', error);
    process.exit(1);
  }
}

await main();
