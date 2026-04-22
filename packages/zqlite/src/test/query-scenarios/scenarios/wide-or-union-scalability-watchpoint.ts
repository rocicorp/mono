import type {QueryScenario} from '../../query-scenario.ts';
import {
  colName,
  createEducationAppTables,
  educationAppRelationships,
  educationAppSchema,
  educationAppTables,
  relationshipName,
  tableName,
} from '../education-app.ts';

const assignment = educationAppTables.assignment;
const assignmentToStudent = educationAppTables.assignment_to_student;
const assignmentToStudentRelationship = relationshipName(
  educationAppRelationships.assignment,
  'assignment_to_student',
);

export default {
  name: 'wide OR union is a scalability watchpoint',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const assignmentToStudent = tables.assignment_to_student;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, i % 7, i % 250 === 0 ? 'archived' : null, i);
    }

    db.prepare(
      `INSERT INTO ${tableName(assignmentToStudent)} (${colName(assignmentToStudent, 'assignment_id')}, ${colName(assignmentToStudent, 'student_id')}, ${colName(assignmentToStudent, 'created_at')}) VALUES (?, ?, ?)`,
    ).run(1_999, 'student-1', 1_999);
  },
  query: builder =>
    builder[assignment.name]
      .where(({cmp, exists, or}) =>
        or(
          cmp(colName(assignment, 'teacher_id'), '=', 1),
          cmp(colName(assignment, 'id'), '=', 1_500),
          cmp(colName(assignment, 'created_at'), '=', 1_750),
          cmp(colName(assignment, 'archived_at'), 'IS', null),
          cmp(colName(assignment, 'teacher_id'), '=', 3),
          exists(assignmentToStudentRelationship, membership =>
            membership.where(
              colName(assignmentToStudent, 'student_id'),
              '=',
              'student-1',
            ),
          ),
        ),
      )
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
  expectations: {
    // Submitted ZQL:
    //
    //   assignment.where(
    //     teacher_id = 1
    //     OR id = pinned
    //     OR created_at = timestamp
    //     OR archived_at IS null
    //     OR teacher_id = 3
    //     OR EXISTS membership(student)
    //   )
    //
    // Current plan:
    //
    //   parent branch 0 ----------------.
    //   parent branch 1 ----------------|
    //   parent branch 2 ----------------+-- InputUnion
    //   parent branch 3 ----------------|
    //   parent branch 4 ----------------|
    //   membership branch -> parent ----'
    //
    // Watchpoint:
    //
    //   This is not a correctness failure, and fetch can be fine for a modest
    //   branch count. The push path still probes sibling branches to preserve
    //   visible-copy semantics, so very wide ORs should eventually be costed or
    //   grouped before lowering to InputUnion.
    sql: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "teacher_id" IN (SELECT value FROM json_each(?)) ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? ORDER BY "created_at" desc, "id" asc',
        calls: 2,
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "created_at" = ? ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "archived_at" IS ? ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
