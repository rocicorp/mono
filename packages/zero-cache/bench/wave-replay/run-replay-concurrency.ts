/**
 * Start `--clients` fresh client groups that each replay the six-root
 * assignment wave concurrently against one zero-cache, then write one summary
 * JSON per run.
 *
 * Each client is a separate bun process so the client groups are genuinely
 * independent: distinct auth token, distinct in-memory kv store, distinct
 * client group id. The server is the only shared resource.
 *
 *   bun run-replay-concurrency.ts --clients 8 --run 1 --output ./results/raw
 */
import {cp, mkdir} from 'node:fs/promises';

const readFlag = (name: string) => {
  const index = process.argv.indexOf(`--${name}`);
  return index === -1 ? undefined : process.argv[index + 1];
};

const clients = Number(readFlag('clients') ?? '1');
const run = Number(readFlag('run') ?? '1');
const zeroCacheURL = readFlag('zero-cache-url') ?? 'http://localhost:49700';
const assignmentID = readFlag('assignment-id') ?? 'assignment_emu_lag_136';
const userID = readFlag('user-id') ?? 'user_emu_lag_teacher';
const email = readFlag('email') ?? 'emu-teacher@goblinsapp.com';
const goblinsRepo = readFlag('goblins-repo') ?? '/workspace/goblins';
const outputDirectory = readFlag('output') ?? `${import.meta.dir}/results/raw`;

// bun resolves `@goblins/zero` by walking up from the script's own directory,
// so the client driver has to live inside the goblins checkout to run at all.
const replayScript = `${goblinsRepo}/.tmp/zero-ordered-replay.ts`;
await mkdir(`${goblinsRepo}/.tmp`, {recursive: true});
await cp(`${import.meta.dir}/client/zero-ordered-replay.ts`, replayScript);
await mkdir(outputDirectory, {recursive: true});

const startedAt = performance.now();
const children = Array.from({length: clients}, (_, index) =>
  Bun.spawn(
    [
      'bun',
      replayScript,
      '--zero-cache-url',
      zeroCacheURL,
      '--auth',
      `emu-session-token-${String(index + 1).padStart(3, '0')}`,
      '--user-id',
      userID,
      '--email',
      email,
      '--assignment-id',
      assignmentID,
      '--mode',
      'wave',
      '--timeout-ms',
      '120000',
    ],
    {stdout: 'pipe', stderr: 'pipe'},
  ),
);

const results = await Promise.all(
  children.map(async (child, index) => {
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ]);
    await Bun.write(
      `${outputDirectory}/n${clients}-run${run}-client${index + 1}.txt`,
      `${stdout}${stderr}`,
    );
    const clientGroup = stdout.match(/client_group=(\S+)/)?.[1];
    const totalSettleSeconds = Number(
      stdout.match(/total settle ([0-9.]+)s/)?.[1],
    );
    // The wave-integrity assertion: one group poke commits every root
    // together, so the six completions must share a millisecond. A wider
    // spread means the reproduction is not exercising single-poke semantics.
    const spreadSeconds = Number(stdout.match(/spread ([0-9.]+)s/)?.[1]);
    const trackerRows = Number(
      stdout.match(/problem_trackers\.for_assignment\s+\S+\s+\S+\s+\S+\s+\S+\s+(\d+)/)
        ?.[1],
    );
    return {
      index: index + 1,
      exitCode,
      clientGroup,
      totalSettleSeconds,
      spreadSeconds,
      trackerRows,
    };
  }),
);

const summary = {
  clients,
  run,
  wallSeconds: (performance.now() - startedAt) / 1000,
  results,
};
await Bun.write(
  `${outputDirectory}/n${clients}-run${run}-summary.json`,
  `${JSON.stringify(summary, null, 2)}\n`,
);
console.log(JSON.stringify(summary));

const failures = results.filter(
  result =>
    result.exitCode !== 0 ||
    !Number.isFinite(result.totalSettleSeconds) ||
    !(result.spreadSeconds <= 0.001),
);
if (failures.length > 0) {
  console.error(
    `wave integrity failed for ${failures.length}/${clients} clients: ${JSON.stringify(failures)}`,
  );
  process.exit(1);
}
