import type {QueryScenario} from '../../query-scenario.ts';
import {
  columnName,
  columnServerName,
  createEducationAppTables,
  educationAppRelationships,
  educationAppSchema,
  educationAppTables,
  relationshipName,
  tableServerName,
} from '../education-app.ts';

const assignment = educationAppTables.assignment;
const assignmentToStudent = educationAppTables.assignment_to_student;
const assignmentToStudentRelationship = relationshipName(
  educationAppRelationships.assignment,
  'assignment_to_student',
);

export default {
  name: 'OR across two membership branches flips both branches',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const assignmentToStudent = tables.assignment_to_student;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableServerName(assignment)} (${columnServerName(assignment, 'id')}, ${columnServerName(assignment, 'teacher_id')}, ${columnServerName(assignment, 'archived_at')}, ${columnServerName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, 2, null, i);
    }

    const membershipStmt = db.prepare(
      `INSERT INTO ${tableServerName(assignmentToStudent)} (${columnServerName(assignmentToStudent, 'assignment_id')}, ${columnServerName(assignmentToStudent, 'student_id')}, ${columnServerName(assignmentToStudent, 'created_at')}) VALUES (?, ?, ?)`,
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
              columnName(assignmentToStudent, 'student_id'),
              '=',
              'student-1',
            ),
          ),
          exists(assignmentToStudentRelationship, q =>
            q.where(
              columnName(assignmentToStudent, 'student_id'),
              '=',
              'student-2',
            ),
          ),
        ),
      )
      .orderBy(columnName(assignment, 'created_at'), 'desc')
      .orderBy(columnName(assignment, 'id'), 'asc'),
  expectations: {
    optimizedAST: {
      where: {
        type: 'or',
        conditions: [
          {
            type: 'correlatedSubquery',
            flip: true,
          },
          {
            type: 'correlatedSubquery',
            flip: true,
          },
        ],
      },
    },
    planDebug: ['flipped'],
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
