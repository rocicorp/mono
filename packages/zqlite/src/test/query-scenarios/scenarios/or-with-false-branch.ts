import type {QueryScenario} from '../../query-scenario.ts';
import {assignmentSchema, seedAssignments} from '../assignment.ts';

export default {
  name: 'OR with false branch keeps surviving teacher predicate',
  schema: assignmentSchema,
  seed: seedAssignments,
  query: builder =>
    builder.assignment
      .where(({cmp, or}) => or(cmp('teacher_id', '=', 1), or()))
      .orderBy('created_at', 'desc')
      .orderBy('id', 'asc'),
  expectations: {
    optimizedAST: {
      where: {
        type: 'simple',
        left: {type: 'column', name: 'teacher_id'},
        op: '=',
        right: {type: 'literal', value: 1},
      },
    },
    sql: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "teacher_id" = ? ORDER BY "created_at" desc, "id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof assignmentSchema>;
