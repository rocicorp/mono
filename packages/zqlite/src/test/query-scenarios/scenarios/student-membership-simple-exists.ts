import type {QueryScenario} from '../../query-scenario.ts';
import {assignmentSchema, seedAssignments} from '../assignment.ts';

export default {
  name: 'simple student membership exists auto flips to membership root',
  schema: assignmentSchema,
  seed: seedAssignments,
  query: builder =>
    builder.assignment
      .where('archived_at', 'IS', null)
      .whereExists('assignment_to_student', q =>
        q.where('student_id', '=', 'student-1'),
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
            type: 'correlatedSubquery',
            flip: true,
          },
        ],
      },
    },
    planDebug: ['flipped'],
    sql: [
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
