/* oxlint-disable no-console */
import {describe, expect, test} from 'vitest';
import {Debug} from '../../../zql/src/builder/debug-delegate.ts';
import type {AnyQuery, Query} from '../../../zql/src/query/query.ts';
import '../helpers/comparePg.ts';
import {bootstrap} from '../helpers/runner.ts';
import {schema} from './schema.ts';
import {generatePgContent, makeSeed, USER_ID} from './seed.ts';

type Schema = typeof schema;

/**
 * Scaling experiment: hold the school topology (32 teachers, 29 classes)
 * constant and double the per-student data (students × memberships ×
 * teacher_student_access) at scales 1, 2, 4, 8. The hypothesis is that the
 * normalized OR + flipped-exists pipeline pays a per-row cost on the
 * student_class_membership stream that grows faster than the denorm form.
 * If the ratio holds, per-row cost is similar; if it grows, the normalized
 * form has worse per-row scaling.
 */

const SCALES = [1, 2, 4, 8] as const;
const RUNS_PER_SCALE = 5;

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

function buildDenormQuery(
  q: Query<'student', Schema>,
): Query<'student', Schema> {
  return q.whereExists(
    'teacherAccess',
    ta =>
      ta.whereExists('teacher', t => t.where('userId', '=', USER_ID), {
        flip: true,
        scalar: true,
      }),
    {flip: true},
  );
}

function timeMs<T>(fn: () => T): [T, number] {
  const start = performance.now();
  const result = fn();
  return [result, performance.now() - start];
}

type Harness = Awaited<ReturnType<typeof bootstrap<Schema>>>;

async function timeQuery(harness: Harness, q: AnyQuery, runs: number) {
  await harness.delegates.sqlite.run(q); // warm-up
  const samples: number[] = [];
  for (let i = 0; i < runs; i++) {
    const [, ms] = timeMs(() => harness.delegates.sqlite.materialize(q));
    samples.push(ms);
  }
  samples.sort((a, b) => a - b);
  return {
    min: samples[0],
    median: samples[Math.floor(samples.length / 2)],
    max: samples.at(-1)!,
  };
}

function totalNvisit(harness: Harness, q: AnyQuery): number {
  const debug = new Debug();
  const prev = harness.delegates.sqlite.debug;
  harness.delegates.sqlite.debug = debug;
  try {
    const view = harness.delegates.sqlite.materialize(q);
    view.destroy();
  } finally {
    harness.delegates.sqlite.debug = prev;
  }
  let total = 0;
  for (const byQuery of Object.values(debug.getNVisitCounts())) {
    for (const v of Object.values(byQuery)) total += v;
  }
  return total;
}

describe('school access query shapes — scaling', {timeout: 600_000}, () => {
  test('ratio across 1x..8x student scale on zqlite', async () => {
    type Row = {
      scale: number;
      students: number;
      memberships: number;
      normMedian: number;
      denormMedian: number;
      ratio: number;
      normNvisit: number;
      denormNvisit: number;
    };
    const rows: Row[] = [];

    for (const scale of SCALES) {
      const seed = makeSeed(scale);
      const pgContent = generatePgContent(seed);
      const harness = await bootstrap({
        suiteName: `school_access_scale_${scale}x`,
        zqlSchema: schema,
        pgContent,
      });

      const studentQuery = harness.queries.student;
      const normalizedQ = buildNormalizedQuery(studentQuery)
        .orderBy('id', 'asc')
        .limit(500);
      const denormQ = buildDenormQuery(studentQuery)
        .orderBy('id', 'asc')
        .limit(500);

      const normNvisit = totalNvisit(harness, normalizedQ);
      const denormNvisit = totalNvisit(harness, denormQ);

      const norm = await timeQuery(harness, normalizedQ, RUNS_PER_SCALE);
      const denorm = await timeQuery(harness, denormQ, RUNS_PER_SCALE);

      rows.push({
        scale,
        students: seed.numStudents,
        memberships: seed.numStudents * seed.membershipsPerStudent,
        normMedian: norm.median,
        denormMedian: denorm.median,
        ratio: norm.median / denorm.median,
        normNvisit,
        denormNvisit,
      });
    }

    console.log('\n=== school-access scaling (zqlite) ===');
    console.log(
      [
        'scale',
        'students',
        'mships',
        'norm.med',
        'denorm.med',
        'ratio',
        'norm.nvisit',
        'denorm.nvisit',
      ]
        .map(s => s.padStart(12))
        .join(' '),
    );
    for (const r of rows) {
      console.log(
        [
          `${r.scale}x`,
          r.students.toString(),
          r.memberships.toString(),
          `${r.normMedian.toFixed(2)}ms`,
          `${r.denormMedian.toFixed(2)}ms`,
          `${r.ratio.toFixed(2)}x`,
          r.normNvisit.toString(),
          r.denormNvisit.toString(),
        ]
          .map(s => s.padStart(12))
          .join(' '),
      );
    }

    // Sanity bounds — primary signal is the log table.
    for (const r of rows) {
      expect(r.normMedian).toBeGreaterThan(0);
      expect(r.denormMedian).toBeGreaterThan(0);
    }
  });
});
