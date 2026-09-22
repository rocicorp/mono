import {readFileSync} from 'node:fs';
import {relative} from 'node:path';
import {BaseSequencer, type TestSpecification} from 'vitest/node';

/**
 * Splits test files across `--shard` runs by expected duration.
 *
 * Vitest's default sequencer sorts files by a hash of their path and slices
 * the list into equal-sized chunks, so a handful of slow files can all land in
 * one shard. This sequencer instead assigns files longest-first to whichever
 * shard currently has the least estimated work, which keeps the shards' total
 * durations close together.
 *
 * Expected durations come from `test-durations.json` at the repo root, keyed by
 * path relative to the root. Files without an entry are assumed to take the
 * median of the known durations. Regenerate the file with
 * `pnpm run update-test-durations` (see scripts/src/update-test-durations.ts).
 */

const DURATIONS_URL = new URL(
  '../../../../test-durations.json',
  import.meta.url,
);

type Durations = Record<string, number>;

function loadDurations(): Durations {
  return JSON.parse(readFileSync(DURATIONS_URL, 'utf8')) as Durations;
}

function median(values: number[]): number {
  if (values.length === 0) {
    return 1000;
  }
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

function compareStrings(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

type Entry = {
  spec: TestSpecification;
  key: string;
  duration: number;
  known: boolean;
};

export class DurationSequencer extends BaseSequencer {
  readonly #durations = loadDurations();
  readonly #defaultDuration = median(Object.values(this.#durations));

  #entry(spec: TestSpecification): Entry {
    const key = relative(this.ctx.config.root, spec.moduleId).replaceAll(
      '\\',
      '/',
    );
    const duration = this.#durations[key];
    return {
      spec,
      key,
      duration: duration ?? this.#defaultDuration,
      known: duration !== undefined,
    };
  }

  // Longest first, with a total order on (key, project) so that every shard
  // job computes the same assignment from the same set of files.
  #sortedEntries(files: TestSpecification[]): Entry[] {
    return files
      .map(spec => this.#entry(spec))
      .sort(
        (a, b) =>
          b.duration - a.duration ||
          compareStrings(a.key, b.key) ||
          compareStrings(a.spec.project.name, b.spec.project.name),
      );
  }

  override shard(files: TestSpecification[]): Promise<TestSpecification[]> {
    const {shard} = this.ctx.config;
    if (!shard) {
      return Promise.resolve(files);
    }
    const {index, count} = shard;
    const entries = this.#sortedEntries(files);

    // Longest-processing-time greedy bin packing.
    const loads: number[] = new Array(count).fill(0);
    const mine: TestSpecification[] = [];
    let unknown = 0;
    for (const entry of entries) {
      let target = 0;
      for (let i = 1; i < count; i++) {
        if (loads[i] < loads[target]) {
          target = i;
        }
      }
      loads[target] += entry.duration;
      if (target === index - 1) {
        mine.push(entry.spec);
        if (!entry.known) {
          unknown++;
        }
      }
    }

    const seconds = (ms: number) => `${(ms / 1000).toFixed(1)}s`;
    this.ctx.logger.log(
      `Shard ${index}/${count}: ${mine.length} of ${files.length} test files, ` +
        `estimated ${seconds(loads[index - 1])} ` +
        `(all shards: ${loads.map(seconds).join(', ')}; ` +
        `${unknown} files missing from test-durations.json)`,
    );
    return Promise.resolve(mine);
  }

  // Run the longest files first so the worker pool packs them well.
  override sort(files: TestSpecification[]): Promise<TestSpecification[]> {
    return Promise.resolve(
      this.#sortedEntries(files)
        .sort(
          (a, b) =>
            a.spec.project.config.sequence.groupOrder -
            b.spec.project.config.sequence.groupOrder,
        )
        .map(entry => entry.spec),
    );
  }
}
