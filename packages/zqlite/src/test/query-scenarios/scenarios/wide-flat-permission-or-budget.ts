import {
  markAllExistsAsPermissions,
  type QueryScenario,
} from '../../query-scenario.ts';
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
  name: 'wide flat permission OR respects root union branch budget',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const assignmentToStudent = tables.assignment_to_student;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, 2, null, i);
    }

    db.prepare(
      `INSERT INTO ${tableName(assignmentToStudent)} (${colName(assignmentToStudent, 'assignment_id')}, ${colName(assignmentToStudent, 'student_id')}, ${colName(assignmentToStudent, 'created_at')}) VALUES (?, ?, ?)`,
    ).run(1_999, 'student-1', 1_999);
  },
  query: builder =>
    builder[assignment.name]
      .where(({cmp, exists, or}) =>
        or(
          ...Array.from({length: 33}, (_, index) =>
            cmp(colName(assignment, 'id'), '=', index + 1),
          ),
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
  transformAST: markAllExistsAsPermissions,
  expectations: {
    // Submitted ZQL:
    //
    //   assignment.where(
    //     id = 1
    //     OR id = 2
    //     OR ...
    //     OR id = 33
    //     OR EXISTS permission_membership(student = currentStudent)
    //   )
    //
    // Tempting plan:
    //
    //   id branch 1 -------------------.
    //   id branch 2 -------------------|
    //   ...                            +-- 34 InputUnion branches
    //   id branch 33 ------------------|
    //   permission membership -> parent'
    //
    // Chosen plan:
    //
    //   assignment(id IN [1..33])
    //     |
    //     `-- permission membership branch stays child-rooted
    //
    // Intuition:
    //
    //   Even when every branch is individually selective, a flat OR can create
    //   a silly number of branch pipelines. The branch budget applies to the
    //   final branch count, not just distributed DNF products.
    sql: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" IN (SELECT value FROM json_each(?)) ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" IN (?) ORDER BY "created_at" desc, "id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
