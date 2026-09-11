/**
 * Run the whole concurrency matrix, then aggregate it into per-N medians.
 *
 * The server log has to be truncated before the matrix starts, because
 * `extract-stage-events.mjs` reads every stage event in the file and
 * `summarize-concurrency.mjs` joins them to client groups by id.
 *
 *   bun run-matrix.ts --server-log ~/work/zcbench/zero-cache.log --output ./results
 */
import {mkdir, writeFile} from 'node:fs/promises';

const readFlag = (name: string) => {
  const index = process.argv.indexOf(`--${name}`);
  return index === -1 ? undefined : process.argv[index + 1];
};

const serverLog = readFlag('server-log');
if (serverLog === undefined) {
  console.error('missing --server-log');
  process.exit(1);
}
const zeroCacheURL = readFlag('zero-cache-url');
const outputDirectory = readFlag('output') ?? `${import.meta.dir}/results`;
const rawDirectory = `${outputDirectory}/raw`;
const runsPerN = Number(readFlag('runs') ?? '3');
const concurrencies = (readFlag('concurrencies') ?? '1,2,4,8,16')
  .split(',')
  .map(Number);
const settleDelayMs = Number(readFlag('settle-delay-ms') ?? '3000');

await mkdir(rawDirectory, {recursive: true});

const runLines: string[] = [];
for (const clients of concurrencies) {
  for (let run = 1; run <= runsPerN; run++) {
    const child = Bun.spawn(
      [
        'bun',
        `${import.meta.dir}/run-replay-concurrency.ts`,
        '--clients',
        String(clients),
        '--run',
        String(run),
        '--output',
        rawDirectory,
        ...(zeroCacheURL === undefined ? [] : ['--zero-cache-url', zeroCacheURL]),
      ],
      {stdout: 'pipe', stderr: 'inherit'},
    );
    const [stdout, exitCode] = await Promise.all([
      new Response(child.stdout).text(),
      child.exited,
    ]);
    if (exitCode !== 0) {
      console.error(`n=${clients} run=${run} failed`);
      process.exit(exitCode);
    }
    const line = stdout.trim().split('\n').at(-1) ?? '';
    runLines.push(line);
    const summary = JSON.parse(line);
    console.log(
      `n=${clients} run=${run} wall=${summary.wallSeconds.toFixed(3)}s settle=[${summary.results
        .map((result: {totalSettleSeconds: number}) =>
          result.totalSettleSeconds.toFixed(3),
        )
        .join(' ')}]`,
    );
    // Let the previous wave's client groups finish tearing down so the next
    // run starts against an idle syncer.
    await Bun.sleep(settleDelayMs);
  }
}

await writeFile(`${rawDirectory}/runs.jsonl`, `${runLines.join('\n')}\n`);

const shell = async (command: string[]) => {
  const child = Bun.spawn(command, {stdout: 'inherit', stderr: 'inherit'});
  const exitCode = await child.exited;
  if (exitCode !== 0) {
    console.error(`failed: ${command.join(' ')}`);
    process.exit(exitCode);
  }
};

await shell([
  'node',
  `${import.meta.dir}/extract-stage-events.mjs`,
  serverLog,
  `${outputDirectory}/stage-events.json`,
]);
await shell([
  'node',
  `${import.meta.dir}/summarize-concurrency.mjs`,
  `${rawDirectory}/runs.jsonl`,
  `${outputDirectory}/stage-events.json`,
  `${outputDirectory}/summary.json`,
]);
