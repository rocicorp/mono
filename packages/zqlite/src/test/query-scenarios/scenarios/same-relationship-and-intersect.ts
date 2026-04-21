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
  name: 'same relationship AND could intersect child scans before parent lookup',
  knownFailure: {
    reason:
      'Two required memberships over the same relationship can be intersected on assignment_id before loading parent rows. The planner currently chooses one child root, loads the parent, then probes the second child relationship.',
    current: `
AND
  exists assignment_to_student where student_id = student-1
  exists assignment_to_student where student_id = student-2

Plan shape today:

assignment_to_student student-1 => assignment => probe assignment_to_student student-2
`,
    desired: `
AND as child intersection:

assignment_to_student student-1
assignment_to_student student-2
both streams grouped by assignment_id
then assignment lookup
`,
    engineIdea:
      'Teach join enumeration about sibling exists clauses that share the same relationship and correlation under AND. It could introduce an intersection node keyed by the child correlation fields before parent lookup.',
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
    membershipStmt.run(102, 'student-2', 103);
    membershipStmt.run(1_500, 'student-2', 1_500);
  },
  query: builder =>
    builder[assignment.name]
      .where(({and, exists}) =>
        and(
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
  expectations: {
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
