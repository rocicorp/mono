/* oxlint-disable no-console */
import {writeFileSync} from 'node:fs';
import * as inspector from 'node:inspector';
import {join} from 'node:path';
import {describe, expect, test} from 'vitest';
import type {AnyQuery, Query} from '../../../zql/src/query/query.ts';
import '../helpers/comparePg.ts';
import {bootstrap} from '../helpers/runner.ts';
import {schema} from './schema.ts';
import {generatePgContent, makeSeed, USER_ID} from './seed.ts';

type Schema = typeof schema;

/**
 * CPU profile of the normalized OR + flipped-exists query at 8x topology
 * (where the OR fan-out cost is most exposed). Captures a .cpuprofile via
 * node:inspector and prints the top self-time hot spots inline so we can
 * see at a glance where IVM dispatch time is going. The .cpuprofile is
 * written to /tmp for opening in Chrome DevTools (chrome://inspect →
 * "Open dedicated DevTools for Node" → Performance → "Load profile…").
 */

const PROFILE_TOPOLOGY_SCALE = 8;
const ITERATIONS = 200;

function buildNormalizedQuery(
  q: Query<'student', Schema>,
): Query<'student', Schema> {
  return q.whereExists(
    'classes',
    sc =>
      sc.whereExists(
        'class',
        c =>
          c.where(({or, exists}) =>
            or(
              exists(
                'teachers',
                tc =>
                  tc.whereExists(
                    'teacher',
                    t => t.where('userId', '=', USER_ID),
                    {flip: true, scalar: true},
                  ),
                {flip: true},
              ),
              exists(
                'teachers',
                tc =>
                  tc.whereExists(
                    'teacher',
                    t =>
                      t.whereExists(
                        'coTeacherGrants',
                        g =>
                          g.whereExists(
                            'toTeacher',
                            co => co.where('userId', '=', USER_ID),
                            {flip: true, scalar: true},
                          ),
                        {flip: true},
                      ),
                    {flip: true},
                  ),
                {flip: true},
              ),
              exists(
                'teachers',
                tc =>
                  tc.whereExists(
                    'teacher',
                    t =>
                      t.whereExists(
                        'school',
                        s =>
                          s.whereExists(
                            'teachers',
                            st =>
                              st.where(({and, cmp}) =>
                                and(
                                  cmp('userId', '=', USER_ID),
                                  cmp('role', '=', 'school-administrator'),
                                ),
                              ),
                            {flip: true, scalar: true},
                          ),
                        {flip: true},
                      ),
                    {flip: true},
                  ),
                {flip: true},
              ),
              exists(
                'teachers',
                tc =>
                  tc.whereExists(
                    'teacher',
                    t =>
                      t.whereExists(
                        'school',
                        s =>
                          s.whereExists(
                            'group',
                            g =>
                              g.whereExists(
                                'schools',
                                ds =>
                                  ds.whereExists(
                                    'teachers',
                                    dt =>
                                      dt.where(({and, cmp}) =>
                                        and(
                                          cmp('userId', '=', USER_ID),
                                          cmp('role', '=', 'administrator'),
                                        ),
                                      ),
                                    {flip: true, scalar: true},
                                  ),
                                {flip: true},
                              ),
                            {flip: true},
                          ),
                        {flip: true},
                      ),
                    {flip: true},
                  ),
                {flip: true},
              ),
            ),
          ),
        {flip: true},
      ),
    {flip: true},
  );
}

function buildClassAccessQuery(
  q: Query<'student', Schema>,
): Query<'student', Schema> {
  return q.whereExists(
    'classes',
    sc =>
      sc.whereExists(
        'class',
        c =>
          c.whereExists(
            'teacherClassAccesses',
            tca =>
              tca.whereExists('teacher', t => t.where('userId', '=', USER_ID), {
                flip: true,
                scalar: true,
              }),
            {flip: true},
          ),
        {flip: true},
      ),
    {flip: true},
  );
}

type CpuProfileNode = {
  id: number;
  callFrame: {
    functionName: string;
    scriptId: string;
    url: string;
    lineNumber: number;
    columnNumber: number;
  };
  hitCount?: number;
  children?: number[];
};

type CpuProfile = {
  nodes: CpuProfileNode[];
  startTime: number;
  endTime: number;
  samples?: number[];
  timeDeltas?: number[];
};

async function captureProfile<T>(fn: () => Promise<T> | T): Promise<{
  result: T;
  profile: CpuProfile;
  elapsedMs: number;
}> {
  const session = new inspector.Session();
  session.connect();

  const post = <U>(method: string, params?: object): Promise<U> =>
    new Promise((resolve, reject) => {
      session.post(method, params ?? {}, (err, value) => {
        if (err) reject(err);
        else resolve(value as U);
      });
    });

  await post('Profiler.enable');
  // 100us sampling — finer than default (1ms) so short pipeline ops aren't lost.
  await post('Profiler.setSamplingInterval', {interval: 100});
  await post('Profiler.start');

  const start = performance.now();
  const result = await fn();
  const elapsedMs = performance.now() - start;

  const {profile} = await post<{profile: CpuProfile}>('Profiler.stop');
  session.disconnect();

  return {result, profile, elapsedMs};
}

type HotEntry = {
  fn: string;
  url: string;
  hits: number;
  selfMs: number;
};

function topHotFrames(
  profile: CpuProfile,
  topN: number,
  filter?: (n: CpuProfileNode) => boolean,
): HotEntry[] {
  const totalHits = profile.nodes.reduce(
    (acc, n) => acc + (n.hitCount ?? 0),
    0,
  );
  const totalElapsedMs = (profile.endTime - profile.startTime) / 1000;

  const entries: HotEntry[] = [];
  for (const n of profile.nodes) {
    const hits = n.hitCount ?? 0;
    if (hits === 0) continue;
    if (filter && !filter(n)) continue;
    const selfMs = (hits / totalHits) * totalElapsedMs;
    const fn = n.callFrame.functionName || '(anonymous)';
    const url = n.callFrame.url;
    entries.push({fn, url, hits, selfMs});
  }
  entries.sort((a, b) => b.selfMs - a.selfMs);
  return entries.slice(0, topN);
}

function shortenUrl(url: string): string {
  if (!url) return '(native)';
  const i = url.indexOf('/packages/');
  return i >= 0 ? url.slice(i + 1) : url;
}

function logHot(label: string, entries: HotEntry[]) {
  console.log(`\n=== ${label} (top ${entries.length} self-time frames) ===`);
  console.log(
    'self ms'.padStart(8) +
      '  hits'.padStart(8) +
      '  ' +
      'function'.padEnd(40) +
      '  source',
  );
  for (const e of entries) {
    console.log(
      e.selfMs.toFixed(2).padStart(8) +
        '  ' +
        e.hits.toString().padStart(6) +
        '  ' +
        e.fn.slice(0, 40).padEnd(40) +
        '  ' +
        shortenUrl(e.url),
    );
  }
}

// Gated on PROFILE=1 so CI doesn't burn time generating .cpuprofile files.
//   PROFILE=1 npm --workspace=zql-integration-tests run test -- \
//     --project=zero-cache/pg-18 school-access-profile
describe.skipIf(!process.env.PROFILE)(
  'school access — CPU profile (normalized vs class-access denorm)',
  {timeout: 600_000},
  () => {
    test(`profile both shapes at ${PROFILE_TOPOLOGY_SCALE}x topology`, async () => {
      const seed = makeSeed({
        studentScale: 1,
        topologyScale: PROFILE_TOPOLOGY_SCALE,
      });
      const harness = await bootstrap({
        suiteName: `school_access_profile_${PROFILE_TOPOLOGY_SCALE}x`,
        zqlSchema: schema,
        pgContent: generatePgContent(seed),
      });

      const normalizedQ = buildNormalizedQuery(harness.queries.student)
        .orderBy('id', 'asc')
        .limit(500);
      const classDenormQ = buildClassAccessQuery(harness.queries.student)
        .orderBy('id', 'asc')
        .limit(500);

      // Warm-up so we don't profile JIT compilation.
      for (let i = 0; i < 5; i++) {
        await harness.delegates.sqlite.run(normalizedQ);
        await harness.delegates.sqlite.run(classDenormQ);
      }

      async function hammer(q: AnyQuery, label: string) {
        const {profile, elapsedMs} = await captureProfile(() => {
          for (let i = 0; i < ITERATIONS; i++) {
            const view = harness.delegates.sqlite.materialize(q);
            view.destroy();
          }
        });

        const outPath = join('/tmp', `school-access-${label}.cpuprofile`);
        writeFileSync(outPath, JSON.stringify(profile));
        console.log(
          `\n[${label}] ${ITERATIONS} iters in ${elapsedMs.toFixed(
            1,
          )}ms  (${(elapsedMs / ITERATIONS).toFixed(2)} ms/iter)  → ${outPath}`,
        );

        // All frames
        logHot(`${label} — all frames`, topHotFrames(profile, 20));

        // Frames in our packages (drops node internals + GC + native)
        logHot(
          `${label} — packages/* only`,
          topHotFrames(profile, 20, n =>
            (n.callFrame.url ?? '').includes('/packages/'),
          ),
        );

        // Frames in the IVM operator code specifically
        logHot(
          `${label} — packages/zql/src/ivm`,
          topHotFrames(profile, 15, n =>
            (n.callFrame.url ?? '').includes('/packages/zql/src/ivm/'),
          ),
        );
      }

      await hammer(normalizedQ, 'normalized');
      await hammer(classDenormQ, 'classDenorm');

      expect(true).toBe(true); // primary signal is the log + .cpuprofile
    });
  },
);
