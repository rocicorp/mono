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
  name: 'same relationship AND intersects child scans before parent lookup',
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
    // Submitted ZQL:
    //
    //   assignment
    //     .whereExists(assignment_to_student, student_id = 'student-1')
    //     .whereExists(assignment_to_student, student_id = 'student-2')
    //
    // Naive plan:
    //
    //   assignment
    //     |-- probe membership for student-1
    //     `-- probe membership for student-2
    //
    // Optimized plan:
    //
    //   assignment_to_student(student_id = 'student-1') -> assignment_ids --.
    //                                                               intersect
    //   assignment_to_student(student_id = 'student-2') -> assignment_ids --'
    //                                                               |
    //                                                               v
    //                                                       fetch assignment
    //
    // Intuition:
    //
    //   The same assignment id must appear in both child streams. The SQL text
    //   has calls: 2 because it runs once per student before intersecting ids.
    sql: [
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
        calls: 2,
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? ORDER BY "created_at" desc, "id" asc',
      },
    ],
    rows: [{id: 102, teacher_id: 2, archived_at: null, created_at: 102}],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
