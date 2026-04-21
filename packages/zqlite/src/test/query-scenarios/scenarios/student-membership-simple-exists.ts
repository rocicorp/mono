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
  name: 'simple student membership exists auto flips to membership root',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const assignmentToStudent = tables.assignment_to_student;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableServerName(assignment)} (${columnServerName(assignment, 'id')}, ${columnServerName(assignment, 'teacher_id')}, ${columnServerName(assignment, 'archived_at')}, ${columnServerName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, i === 4 ? 1 : 2, null, i);
    }

    const membershipStmt = db.prepare(
      `INSERT INTO ${tableServerName(assignmentToStudent)} (${columnServerName(assignmentToStudent, 'assignment_id')}, ${columnServerName(assignmentToStudent, 'student_id')}, ${columnServerName(assignmentToStudent, 'created_at')}) VALUES (?, ?, ?)`,
    );
    membershipStmt.run(101, 'student-1', 101);
    membershipStmt.run(102, 'student-1', 102);
    membershipStmt.run(103, 'student-1', 103);
  },
  query: builder =>
    builder[assignment.name]
      .where(columnName(assignment, 'archived_at'), 'IS', null)
      .whereExists(assignmentToStudentRelationship, q =>
        q.where(
          columnName(assignmentToStudent, 'student_id'),
          '=',
          'student-1',
        ),
      )
      .orderBy(columnName(assignment, 'created_at'), 'desc')
      .orderBy(columnName(assignment, 'id'), 'asc'),
  expectations: {
    optimizedAST: {
      where: {
        type: 'and',
        conditions: [
          {},
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
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? AND "archived_at" IS ? ORDER BY "created_at" desc, "id" asc',
      },
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
