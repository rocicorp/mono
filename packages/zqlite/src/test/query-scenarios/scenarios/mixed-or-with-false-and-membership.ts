import type {QueryScenario} from '../../query-scenario.ts';
import {assignmentSchema, seedAssignments} from '../assignment.ts';

export default {
  name: 'mixed OR ignores false branch and auto flips membership branch',
  schema: assignmentSchema,
  seed: seedAssignments,
  query: builder =>
    builder.assignment
      .where(({and, cmp, exists, or}) =>
        and(
          cmp('archived_at', 'IS', null),
          or(
            cmp('teacher_id', '=', 1),
            or(),
            exists('assignment_to_student', q =>
              q.where('student_id', '=', 'student-1'),
            ),
          ),
        ),
      )
      .orderBy('created_at', 'desc')
      .orderBy('id', 'asc'),
  expectations: {
    optimizedAST: {
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
    },
    planDebug: ['flipped'],
    sql: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "archived_at" IS ? ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? AND "archived_at" IS ? ORDER BY "created_at" desc, "id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof assignmentSchema>;
