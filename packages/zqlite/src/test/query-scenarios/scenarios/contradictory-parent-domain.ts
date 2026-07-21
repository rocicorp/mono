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
  name: 'contradictory same column parent filters collapse to false',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 100; i++) {
      assignmentStmt.run(i, i % 2 === 0 ? 1 : 2, null, i);
    }
  },
  query: builder =>
    builder[assignment.name]
      .where(({and, cmp}) =>
        and(
          cmp(colName(assignment, 'teacher_id'), '=', 1),
          cmp(colName(assignment, 'teacher_id'), '!=', 1),
        ),
      )
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
  expectations: {
    optimizedAST: {
      where: {
        type: 'or',
        conditions: [],
      },
    },
    // Submitted ZQL:
    //
    //   assignment.where(teacher_id = 1 AND teacher_id != 1)
    //
    // Naive plan:
    //
    //   assignment
    //     `-- scan rows and test both predicates one row at a time
    //
    // Optimized plan:
    //
    //   teacher_id = 1 AND teacher_id != 1
    //              |
    //              v
    //            FALSE
    //
    // Intuition:
    //
    //   No row can be both teacher 1 and not teacher 1, so the planner emits
    //   an empty query instead of scanning assignment.
    sql: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE FALSE ORDER BY "created_at" desc, "id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
