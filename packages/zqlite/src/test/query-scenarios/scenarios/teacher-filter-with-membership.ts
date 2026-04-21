import type {QueryScenario} from '../../query-scenario.ts';
import {assignmentSchema, seedAssignments} from '../assignment.ts';

export default {
  name: 'selective teacher filter keeps membership exists unflipped',
  schema: assignmentSchema,
  seed: seedAssignments,
  query: builder =>
    builder.assignment
      .where('archived_at', 'IS', null)
      .where('teacher_id', '=', 1)
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
          {},
          {
            type: 'correlatedSubquery',
          },
        ],
      },
    },
    planDebug: ['semi'],
    sql: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE ("archived_at" IS ? AND "teacher_id" = ?) ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "assignment_id" = ? AND "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof assignmentSchema>;
