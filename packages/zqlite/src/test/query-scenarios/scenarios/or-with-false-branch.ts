import type {QueryScenario} from '../../query-scenario.ts';
import {
  educationAppSchema,
  createEducationAppTables,
} from '../education-app.ts';

export default {
  name: 'OR with false branch keeps surviving teacher predicate',
  schema: educationAppSchema,
  seed: db => {
    createEducationAppTables(db);

    const assignmentStmt = db.prepare(
      'INSERT INTO assignment (id, teacher_id, archived_at, created_at) VALUES (?, ?, ?, ?)',
    );
    assignmentStmt.run(1, 1, null, 1);
    assignmentStmt.run(2, 2, null, 2);
  },
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
} satisfies QueryScenario<typeof educationAppSchema>;
