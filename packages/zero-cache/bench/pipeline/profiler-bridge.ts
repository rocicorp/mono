import {mkdir} from 'node:fs/promises';
import type {BenchmarkConfig} from './config.ts';

export async function ensureOutputDir(dir: string): Promise<string> {
  await mkdir(dir, {recursive: true});
  return dir;
}

/**
 * Returns environment variables to inject into a child worker process
 * for Node.js V8 CPU profiling if enabled for that worker type.
 */
export function getWorkerProfilingEnv(
  workerType: 'rm' | 'vs',
  workerIndex: number,
  config: BenchmarkConfig,
): NodeJS.ProcessEnv {
  const shouldProfile =
    (workerType === 'rm' && config.profileReplicationManager) ||
    (workerType === 'vs' && config.profileViewSyncer);

  if (!shouldProfile) {
    return {};
  }

  const profileName = `${workerType}_${workerIndex}_${Date.now()}.cpuprofile`;
  const existingNodeOptions = process.env.NODE_OPTIONS ?? '';
  const profilerFlag = `--cpu-prof --cpu-prof-dir=${config.outputDir} --cpu-prof-name=${profileName}`;

  return {
    NODE_OPTIONS: `${existingNodeOptions} ${profilerFlag}`.trim(),
  };
}
