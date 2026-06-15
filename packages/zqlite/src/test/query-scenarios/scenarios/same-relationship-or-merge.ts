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
  name: 'permission system same relationship OR merges into one flipped membership scan',
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

    const membershipStmt = db.prepare(
      `INSERT INTO ${tableName(assignmentToStudent)} (${colName(assignmentToStudent, 'assignment_id')}, ${colName(assignmentToStudent, 'student_id')}, ${colName(assignmentToStudent, 'created_at')}) VALUES (?, ?, ?)`,
    );
    membershipStmt.run(101, 'student-1', 101);
    membershipStmt.run(102, 'student-1', 102);
    membershipStmt.run(1_500, 'student-2', 1_500);
  },
  query: builder =>
    builder[assignment.name]
      .where(({exists, or}) =>
        or(
          exists(assignmentToStudentRelationship, q =>
            q.where(
              colName(assignmentToStudent, 'student_id'),
              '=',
              'student-1',
            ),
          ),
          exists(assignmentToStudentRelationship, q =>
            q.where(
              colName(assignmentToStudent, 'student_id'),
              '=',
              'student-2',
            ),
          ),
        ),
      )
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
  transformAST: markAllExistsAsPermissions,
  expectations: {
    optimizedAST: {
      where: {
        type: 'correlatedSubquery',
        flip: true,
      },
    },
    // Submitted ZQL:
    //
    //   assignment.where(
    //     EXISTS permission_membership(student_id = 'student-1')
    //     OR EXISTS permission_membership(student_id = 'student-2')
    //   )
    //
    // Naive plan:
    //
    //   assignment
    //     |-- probe permission membership for student-1
    //     `-- probe permission membership for student-2
    //
    // Optimized plan:
    //
    //   EXISTS permission_membership(student_id = 'student-1')
    //      OR
    //   EXISTS permission_membership(student_id = 'student-2')
    //              |
    //              v
    //   EXISTS permission_membership(student_id IN ['student-1', 'student-2'])
    //
    //   assignment_to_student(student_id IN ['student-1', 'student-2'])
    //     `-- fetch assignment by assignment_id
    //
    // Intuition:
    //
    //   Permission helper rows are private evidence, so merging the two
    //   branches cannot shrink the synced client row set. The optimizer can
    //   use one child-domain scan instead of two.
    sql: [
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" IN (SELECT value FROM json_each(?)) ORDER BY "assignment_id" asc, "student_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" IN (?,?,?) ORDER BY "created_at" desc, "id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
