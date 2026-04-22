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
  name: 'non unique child ids block sibling intersection',
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
    membershipStmt.run(101, 'student-2', 102);
    membershipStmt.run(102, 'student-1', 103);
    membershipStmt.run(103, 'student-2', 104);
    membershipStmt.run(1_500, 'student-3', 1_500);
  },
  query: builder =>
    builder[assignment.name]
      .whereExists(assignmentToStudentRelationship, membership =>
        membership.where(colName(assignmentToStudent, 'student_id'), 'IN', [
          'student-1',
          'student-2',
        ]),
      )
      .whereExists(assignmentToStudentRelationship, membership =>
        membership.where(colName(assignmentToStudent, 'created_at'), '>', 101),
      )
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
  expectations: {
    // Submitted ZQL:
    //
    //   assignment
    //     .whereExists(membership, student_id IN [student-1, student-2])
    //     .whereExists(membership, created_at > 101)
    //
    // Current plan:
    //
    //   membership(student_id IN [...])
    //     `-- fetch assignment
    //           `-- probe membership(created_at > 101) by assignment_id
    //
    // Desired plan:
    //
    //   membership(student_id IN [...]) -- distinct assignment_id --.
    //                                                           +-- intersect ids
    //   membership(created_at > 101) -- distinct assignment_id ----'
    //
    // Intuition:
    //
    //   The current intersection operator asks for one child row per parent id.
    //   This query can produce multiple membership rows for the same assignment,
    //   but EXISTS only needs the distinct assignment ids.
    sql: [
      {
        table: 'assignment_to_student',
        sql: 'desired: intersect distinct assignment ids from both membership scans',
      },
    ],
  },
  knownFailure: {
    reason:
      'Sibling intersection currently requires each child branch to prove uniqueness per parent id.',
    current:
      'The engine starts from one membership branch and probes the other branch per parent id.',
    desired:
      'Materialize distinct assignment ids for each child predicate, then intersect those id sets.',
    currentSQL: [
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" IN (SELECT value FROM json_each(?)) ORDER BY "assignment_id" asc, "student_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? AND TRUE ORDER BY "created_at" desc, "id" asc',
        calls: 4,
      },
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "assignment_id" = ? AND "created_at" > ? ORDER BY "assignment_id" asc, "student_id" asc',
        calls: 7,
      },
    ],
    engineIdea:
      'Add a key-set intersection path that dedupes child rows by correlation key before intersecting.',
  },
} satisfies QueryScenario<typeof educationAppSchema>;
