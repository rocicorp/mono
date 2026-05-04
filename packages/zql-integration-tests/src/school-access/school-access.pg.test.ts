/* oxlint-disable no-console */
import {describe, expect, test} from 'vitest';
import type {AnyQuery, Query} from '../../../zql/src/query/query.ts';
import {mapResultToClientNames} from '../../../zqlite/src/test/source-factory.ts';
import '../helpers/comparePg.ts';
import {bootstrap, runAndCompare} from '../helpers/runner.ts';
import {schema} from './schema.ts';
import {generatePgContent, SEED, USER_ID} from './seed.ts';

type Schema = typeof schema;

/**
 * Customer-reported queries (https://...) re-shaped to flex two access patterns:
 *  - Normalized: 4-branch OR with deeply nested flipped+scalar EXISTS
 *  - Denormalized: single flipped+scalar EXISTS through teacher_student_access
 *
 * Both should return the same set of students. The normalized form was reported
 * as significantly slower (~3x) on zqlite at customer-scale data; this test
 * exists primarily to give us a fast, deterministic reproduction we can profile.
 */

const pgContent = generatePgContent();

const harness = await bootstrap({
  suiteName: 'school_access_profile',
  zqlSchema: schema,
  pgContent,
});

const studentQuery = harness.queries.student;

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
              // 1. Direct: I teach a class the student is in
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

              // 2. Co-teacher: granted access by someone who teaches the class
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

              // 3. School admin: school-administrator at the school
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

              // 4. District admin: administrator in the district
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

async function profile(label: string, q: AnyQuery, runs: number) {
  // warm-up
  await harness.delegates.sqlite.run(q);

  const samples: number[] = [];
  for (let i = 0; i < runs; i++) {
    const [, ms] = timeMs(() => harness.delegates.sqlite.materialize(q));
    samples.push(ms);
  }
  samples.sort((a, b) => a - b);
  const min = samples[0];
  const median = samples[Math.floor(samples.length / 2)];
  const max = samples.at(-1)!;
  console.log(
    `[${label}] min=${min.toFixed(2)}ms  median=${median.toFixed(
      2,
    )}ms  max=${max.toFixed(2)}ms  (${runs} runs)`,
  );
  return {min, median, max};
}

describe('school access query shapes', {timeout: 120_000}, () => {
  test('normalized OR + flip + scalar matches PG (no limit)', async () => {
    const q = buildNormalizedQuery(studentQuery).orderBy('id', 'asc');
    await runAndCompare(schema, harness.delegates, q, undefined);
    const result = await harness.delegates.sqlite.run(q);
    expect(result.length).toBe(SEED.numStudents);
  });

  test('denormalized matches PG (no limit)', async () => {
    const q = buildDenormQuery(studentQuery).orderBy('id', 'asc');
    await runAndCompare(schema, harness.delegates, q, undefined);
    const result = await harness.delegates.sqlite.run(q);
    expect(result.length).toBe(SEED.numStudents);
  });

  test('normalized result equals denormalized result', async () => {
    const normalized = mapResultToClientNames(
      await harness.delegates.sqlite.run(
        buildNormalizedQuery(studentQuery).orderBy('id', 'asc'),
      ),
      schema,
      'student',
    );
    const denorm = mapResultToClientNames(
      await harness.delegates.sqlite.run(
        buildDenormQuery(studentQuery).orderBy('id', 'asc'),
      ),
      schema,
      'student',
    );
    expect(normalized).toEqual(denorm);
  });

  test('profile: normalized vs denorm with limit(500) on zqlite', async () => {
    const normalizedQ = buildNormalizedQuery(studentQuery)
      .orderBy('id', 'asc')
      .limit(500);
    const denormQ = buildDenormQuery(studentQuery)
      .orderBy('id', 'asc')
      .limit(500);

    const RUNS = 5;
    const norm = await profile('normalized', normalizedQ, RUNS);
    const denorm = await profile('denorm    ', denormQ, RUNS);

    console.log(
      `[ratio] normalized.median / denorm.median = ${(
        norm.median / denorm.median
      ).toFixed(2)}x`,
    );

    // Loose sanity bound to catch regressions; primary purpose is the log.
    expect(norm.median).toBeGreaterThan(0);
    expect(denorm.median).toBeGreaterThan(0);
  });
});
