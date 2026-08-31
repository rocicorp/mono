import type {QueryScenario} from '../../query-scenario.ts';
import {
  colName,
  createEducationAppTables,
  educationAppSchema,
  educationAppTables,
  tableName,
} from '../education-app.ts';

const assignment = educationAppTables.assignment;

export default {
  name: 'OR with false branch keeps surviving teacher predicate',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    assignmentStmt.run(1, 1, null, 1);
    assignmentStmt.run(2, 2, null, 2);
  },
  query: builder =>
    builder[assignment.name]
      .where(({cmp, or}) =>
        or(cmp(colName(assignment, 'teacher_id'), '=', 1), or()),
      )
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
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
