import {existsSync, readFileSync} from 'node:fs';
import {dirname, relative, resolve} from 'node:path';
import {describe, expect, test} from 'vitest';

/**
 * Fake timers patch the globals only. `setTimeout` imported from `timers`,
 * `node:timers`, or `node:timers/promises` stays real in every import form, and
 * a real `clearInterval` handed a fake handle silently does nothing. Neither is
 * visible to the determinism guard, so this walks the simulation's import graph
 * and fails on any module that imports them, unless it is on the reviewed list
 * of modules the simulation imports but never runs.
 */
const ROOTS = [
  'simulation.ts',
  'rm.sim.test.ts',
  'determinism.sim.test.ts',
  'sim-pg.sim.test.ts',
];

const NEVER_RUN = new Set([
  // Polls a restored replica for litestream v3; no simulated node builds one.
  'src/services/change-streamer/replica-poller.ts',
]);

const TIMERS = new Set([
  'timers',
  'node:timers',
  'timers/promises',
  'node:timers/promises',
]);

const IMPORT_SPECIFIER =
  /(?:\bimport|\bexport)\s[^;'"]*?\bfrom\s*['"]([^'"]+)['"]|\bimport\s*\(\s*['"]([^'"]+)['"]\s*\)|\bimport\s+['"]([^'"]+)['"]/g;

const PACKAGE_ROOT = resolve(import.meta.dirname, '../../..');

function importsOf(file: string): string[] {
  const source = readFileSync(file, 'utf8');
  return Array.from(
    source.matchAll(IMPORT_SPECIFIER),
    match => match[1] ?? match[2] ?? match[3],
  );
}

describe('sim/timers', () => {
  test('nothing the simulation runs imports timers from node:timers', () => {
    const seen = new Set<string>();
    const offenders: string[] = [];
    const pending = ROOTS.map(root => resolve(import.meta.dirname, root));
    while (pending.length) {
      const file = pending.pop() as string;
      if (seen.has(file)) {
        continue;
      }
      seen.add(file);
      for (const specifier of importsOf(file)) {
        if (TIMERS.has(specifier)) {
          const name = relative(PACKAGE_ROOT, file);
          if (!NEVER_RUN.has(name)) {
            offenders.push(`${name} imports ${specifier}`);
          }
        } else if (specifier.startsWith('.') && specifier.endsWith('.ts')) {
          // The pattern also matches imports quoted in comments, which may
          // name no real file.
          const imported = resolve(dirname(file), specifier);
          if (existsSync(imported)) {
            pending.push(imported);
          }
        }
      }
    }
    expect(seen.size).toBeGreaterThan(100);
    expect(offenders).toEqual([]);
  });
});
