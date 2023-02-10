import {readFile, writeFile} from 'fs/promises';
import {argv} from 'process';

if (argv.length !== 5) {
  console.error(
    'Usage: node split-perf.js <input-file> <perf-file> <p95-file>',
  );
  process.exit(1);
}

const inputFile = argv[2];
const perfFile = argv[3];
const p95File = argv[4];

const inputJSON = JSON.parse(await readFile(inputFile, 'utf8'));

const perfJSON = [];
const p95JSON = [];

for (const entry of inputJSON) {
  if (/p95/.test(entry.unit)) {
    p95JSON.push(entry);
  } else {
    perfJSON.push(entry);
  }
}

await writeFile(perfFile, JSON.stringify(perfJSON, null, 2));
await writeFile(p95File, JSON.stringify(p95JSON, null, 2));
