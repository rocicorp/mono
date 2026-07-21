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
    // Submitted ZQL:
    //
    //   assignment.where(teacher_id = 1 OR FALSE)
    //
    // Naive plan:
    //
    //   assignment
    //     `-- check teacher_id = 1
    //     `-- also carry a FALSE branch that can never match
    //
    // Optimized plan:
    //
    //   teacher_id = 1 OR FALSE
    //              |
    //              v
    //         teacher_id = 1
    //
    // Intuition:
    //
    //   FALSE contributes no rows to an OR, so it should not make costing
    //   think this query is less selective than the real teacher filter.
    sql: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "teacher_id" = ? ORDER BY "created_at" desc, "id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
