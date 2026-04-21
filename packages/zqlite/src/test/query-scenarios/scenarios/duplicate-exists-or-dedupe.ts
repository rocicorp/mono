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
  name: 'duplicate exists OR could dedupe before planning',
  knownFailure: {
    reason:
      'A OR A is exactly A, but duplicate exists branches still reach join enumeration as separate correlated subqueries.',
    current: `
OR
  exists assignment_to_student where student_id = student-1
  exists assignment_to_student where student_id = student-1

Plan shape today:

two equivalent exists branches enter the planner
`,
    desired: `
Deduped predicate:

exists assignment_to_student where student_id = student-1

Desired plan shape:

assignment_to_student student-1 => assignment
`,
    engineIdea:
      'Add canonical condition keys during boolean normalization. Use them to collapse duplicate OR branches and duplicate AND branches before costing.',
  },
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
    membershipStmt.run(103, 'student-1', 103);
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
              'student-1',
            ),
          ),
        ),
      )
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
  expectations: {
    optimizedAST: {
      where: {
        type: 'correlatedSubquery',
        flip: true,
      },
    },
    sql: [
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? ORDER BY "created_at" desc, "id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
