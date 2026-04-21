import {expect, test} from 'vitest';
import {studentMembershipMixedORScenario} from './test/assignment-scenarios.ts';
import {runQueryScenario} from './test/query-scenario.ts';

test('mixed OR costing lets the planner automatically flip a selective membership branch', () => {
  const result = runQueryScenario(studentMembershipMixedORScenario);

  expect(result.optimizedAST).toMatchObject({
    where: {
      type: 'and',
      conditions: [
        {},
        {
          type: 'or',
          conditions: [
            {},
            {
              type: 'correlatedSubquery',
              flip: true,
            },
          ],
        },
      ],
    },
  });
  expect(result.planDebug).toContain('Best plan: Attempt 2');
  expect(result.planDebug).toContain('FO ⋈ assignment_to_student: flipped');
  expect(result.sql).toMatchInlineSnapshot(`
    [
      {
        "sql": "SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "archived_at" IS ? ORDER BY "created_at" desc, "id" asc",
        "table": "assignment",
      },
      {
        "sql": "SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc",
        "table": "assignment_to_student",
      },
      {
        "sql": "SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? AND "archived_at" IS ? ORDER BY "created_at" desc, "id" asc",
        "table": "assignment",
      },
    ]
  `);
});
