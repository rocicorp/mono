import type {QueryScenario} from '../../query-scenario.ts';
import {assignmentSchema, seedAssignments} from '../assignment.ts';

export default {
  name: 'OR across two membership branches flips both branches',
  schema: assignmentSchema,
  seed: seedAssignments,
  query: builder =>
    builder.assignment
      .where(({exists, or}) =>
        or(
          exists('assignment_to_student', q =>
            q.where('student_id', '=', 'student-1'),
          ),
          exists('assignment_to_student', q =>
            q.where('student_id', '=', 'student-2'),
          ),
        ),
      )
      .orderBy('created_at', 'desc')
      .orderBy('id', 'asc'),
  expectations: {
    optimizedAST: {
      where: {
        type: 'or',
        conditions: [
          {
            type: 'correlatedSubquery',
            flip: true,
          },
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
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? ORDER BY "created_at" desc, "id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof assignmentSchema>;
