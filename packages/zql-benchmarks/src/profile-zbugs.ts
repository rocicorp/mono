/* eslint-disable no-console */
import {CpuProfiler} from '../../zero-cache/src/types/profiler.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import type {LiteAndZqlSpec} from '../../zero-cache/src/db/specs.ts';
import {Database} from '../../zqlite/src/db.ts';
import {newQueryDelegate} from '../../zqlite/src/test/source-factory.ts';
import {schema, builder} from './schema.ts';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {computeZqlSpecs} from '../../zero-cache/src/db/lite-tables.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {rename} from 'node:fs/promises';

// Configuration
const ITERATIONS = parseInt(process.argv[2] || '1000', 10);
const WARMUP_ITERATIONS = 10;
const PROFILE_NAME = 'zbugs-exists-query';

console.log(
  `Starting profiling with ${ITERATIONS} iterations (${WARMUP_ITERATIONS} warmup)...`,
);

// Open the zbugs SQLite database
const db = new Database(
  createSilentLogContext(),
  '/Users/mlaw/workspace/mono/apps/zbugs/zbugs-replica.db',
);
const lc = createSilentLogContext();

// Run ANALYZE to populate SQLite statistics for cost model
db.exec('ANALYZE;');

// Get table specs using computeZqlSpecs
const tableSpecs = new Map<string, LiteAndZqlSpec>();
computeZqlSpecs(createSilentLogContext(), db, tableSpecs);

// Create SQLite delegate
const delegate = newQueryDelegate(lc, testLogConfig, db, schema);

// Define the query to profile
const query = builder.issue.whereExists('creator', q => q.where('name', 'sdf'));

// Warmup phase (don't profile this)
console.log('Warming up...');
for (let i = 0; i < WARMUP_ITERATIONS; i++) {
  await delegate.run(query as AnyQuery);
}

console.log('Warmup complete. Starting profiling...');

// Start profiling
const profiler = await CpuProfiler.connect();
await profiler.start();

const startTime = performance.now();

// Run the query multiple times
for (let i = 0; i < ITERATIONS; i++) {
  await delegate.run(query as AnyQuery);
}

const duration = performance.now() - startTime;

// Stop profiling and save to temp directory
await profiler.stopAndDispose(lc, PROFILE_NAME);

// Move the profile from tmpdir to current working directory
const tmpPath = join(tmpdir(), `${PROFILE_NAME}.cpuprofile`);
const cwdPath = join(process.cwd(), `${PROFILE_NAME}.cpuprofile`);
await rename(tmpPath, cwdPath);

console.log(`\nProfiling complete!`);
console.log(`Total time: ${duration.toFixed(2)}ms`);
console.log(
  `Average time per iteration: ${(duration / ITERATIONS).toFixed(2)}ms`,
);
console.log(`\nProfile saved to: ${cwdPath}`);
console.log(`\nTo view the flame graph:`);
console.log(`  1. Chrome DevTools: Open DevTools > Performance > Load Profile`);
console.log(`  2. Speedscope: npx speedscope ${cwdPath}`);
console.log(
  `  3. VS Code: Install "vscode-js-profile-flame" extension and open the .cpuprofile file`,
);

db.close();
