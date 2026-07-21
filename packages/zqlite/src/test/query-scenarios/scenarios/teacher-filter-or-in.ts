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
  name: 'same column parent OR compacts to an IN filter',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, i % 25 === 0 ? 1 : 2, null, i);
    }
  },
  query: builder =>
    builder[assignment.name]
      .where(({cmp, or}) =>
        or(
          cmp(colName(assignment, 'teacher_id'), '=', 1),
          cmp(colName(assignment, 'teacher_id'), '=', 2),
        ),
      )
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
  expectations: {
    optimizedAST: {
      where: {
        type: 'simple',
        left: {type: 'column', name: 'teacher_id'},
        op: 'IN',
        right: {type: 'literal', value: [1, 2]},
      },
    },
    // Submitted ZQL:
    //
    //   assignment.where(teacher_id = 1 OR teacher_id = 2)
    //
    // Naive plan:
    //
    //   assignment
    //     `-- test teacher_id = 1
    //     `-- test teacher_id = 2
    //
    // Optimized plan:
    //
    //   teacher_id = 1 OR teacher_id = 2
    //              |
    //              v
    //        teacher_id IN [1, 2]
    //
    // Intuition:
    //
    //   Two equality checks on the same column are one indexed IN lookup.
    sql: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "teacher_id" IN (SELECT value FROM json_each(?)) ORDER BY "created_at" desc, "id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
