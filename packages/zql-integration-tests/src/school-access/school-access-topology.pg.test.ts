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
 * Topology scaling: hold student data constant (1x → 564 students, 1128
 * memberships) and scale numClasses + numTeachers proportionally. The
 * normalized OR branches do work that's a function of the school topology
 * (every class fans out into 4 OR branches that walk the school's
 * teacher_to_class / teacher_to_co_teacher / school.teachers tables), not
 * the per-student data. So if our earlier hypothesis is right —
 * "normalized's overhead is constant per-school, amortized over students" —
 * the ratio should GROW on this axis.
 */

const TOPOLOGY_SCALES = [1, 2, 4, 8] as const;
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

/**
 * Mid-denorm: stop at the class level instead of cross-joining with
 * student_class_membership. Replaces the 4-branch OR with one linear
 * chain through teacher_class_access. Trades a per-class fan-in for a
 * dramatically smaller denorm table (no student-enrollment fanout).
 */
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

describe(
  'school access query shapes — topology scaling',
  {timeout: 600_000},
  () => {
    test('ratio across 1x..8x topology scale (constant student data)', async () => {
      type Row = {
        topo: number;
        classes: number;
        normMedian: number;
        classDenormMedian: number;
        studentDenormMedian: number;
        normVsStudent: number;
        normVsClass: number;
        classVsStudent: number;
        normNvisit: number;
        classNvisit: number;
        studentNvisit: number;
      };
      const rows: Row[] = [];

      for (const topologyScale of TOPOLOGY_SCALES) {
        const seed = makeSeed({studentScale: 1, topologyScale});
        const pgContent = generatePgContent(seed);
        const harness = await bootstrap({
          suiteName: `school_access_topology_${topologyScale}x`,
          zqlSchema: schema,
          pgContent,
        });

        const studentQuery = harness.queries.student;
        const normalizedQ = buildNormalizedQuery(studentQuery)
          .orderBy('id', 'asc')
          .limit(500);
        const classDenormQ = buildClassAccessQuery(studentQuery)
          .orderBy('id', 'asc')
          .limit(500);
        const studentDenormQ = buildDenormQuery(studentQuery)
          .orderBy('id', 'asc')
          .limit(500);

        const normNvisit = totalNvisit(harness, normalizedQ);
        const classNvisit = totalNvisit(harness, classDenormQ);
        const studentNvisit = totalNvisit(harness, studentDenormQ);

        const norm = await timeQuery(harness, normalizedQ, RUNS_PER_SCALE);
        const classDenorm = await timeQuery(
          harness,
          classDenormQ,
          RUNS_PER_SCALE,
        );
        const studentDenorm = await timeQuery(
          harness,
          studentDenormQ,
          RUNS_PER_SCALE,
        );

        rows.push({
          topo: topologyScale,
          classes: seed.numClasses,
          normMedian: norm.median,
          classDenormMedian: classDenorm.median,
          studentDenormMedian: studentDenorm.median,
          normVsStudent: norm.median / studentDenorm.median,
          normVsClass: norm.median / classDenorm.median,
          classVsStudent: classDenorm.median / studentDenorm.median,
          normNvisit,
          classNvisit,
          studentNvisit,
        });
      }

      console.log('\n=== school-access topology scaling (zqlite) ===');
      const cols = [
        'topo',
        'classes',
        'norm',
        'classDen',
        'studDen',
        'norm/stud',
        'norm/class',
        'class/stud',
        'norm.nvi',
        'class.nvi',
        'stud.nvi',
      ];
      console.log(cols.map(s => s.padStart(12)).join(' '));
      for (const r of rows) {
        console.log(
          [
            `${r.topo}x`,
            r.classes.toString(),
            `${r.normMedian.toFixed(2)}ms`,
            `${r.classDenormMedian.toFixed(2)}ms`,
            `${r.studentDenormMedian.toFixed(2)}ms`,
            `${r.normVsStudent.toFixed(2)}x`,
            `${r.normVsClass.toFixed(2)}x`,
            `${r.classVsStudent.toFixed(2)}x`,
            r.normNvisit.toString(),
            r.classNvisit.toString(),
            r.studentNvisit.toString(),
          ]
            .map(s => s.padStart(12))
            .join(' '),
        );
      }

      for (const r of rows) {
        expect(r.normMedian).toBeGreaterThan(0);
        expect(r.classDenormMedian).toBeGreaterThan(0);
        expect(r.studentDenormMedian).toBeGreaterThan(0);
      }
    });
  },
);
