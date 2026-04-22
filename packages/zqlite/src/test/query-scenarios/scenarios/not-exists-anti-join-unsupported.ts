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
  name: 'NOT EXISTS anti join is still parent rooted',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const assignmentToStudent = tables.assignment_to_student;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 5; i++) {
      assignmentStmt.run(i, 1, null, i);
    }

    db.prepare(
      `INSERT INTO ${tableName(assignmentToStudent)} (${colName(assignmentToStudent, 'assignment_id')}, ${colName(assignmentToStudent, 'student_id')}, ${colName(assignmentToStudent, 'created_at')}) VALUES (?, ?, ?)`,
    ).run(1, 'student-1', 1);
  },
  query: builder =>
    builder[assignment.name]
      .where(({exists, not}) =>
        not(
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
    //     NOT EXISTS assignment_to_student(student_id = selectedStudent)
    //   )
    //
    // Current plan:
    //
    //   assignment
    //     `-- probe membership by assignment_id
    //
    // Desired plan:
    //
    //   assignment
    //     `-- anti semi join assignment_to_student(student_id = selectedStudent)
    //
    // Intuition:
    //
    //   Positive EXISTS can start from the child table because matching child
    //   rows prove parent ids to include. NOT EXISTS needs the opposite: a
    //   parent row is visible only while no matching child row exists, and
    //   child pushes can remove already-visible parents.
    planDebug: ['desired: anti semi join'],
  },
  knownFailure: {
    reason:
      'NOT EXISTS cannot be child-rooted by the current positive EXISTS planner.',
    current:
      'The engine scans parent assignments and probes the child table for absence row by row.',
    desired:
      'Support an anti semi join operator that maintains parent rows with no matching child rows.',
    currentSQL: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "assignment_id" = ? AND "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
        calls: 5,
      },
    ],
    engineIdea:
      'Add an anti-join IVM operator with push semantics for first-match add and last-match remove.',
  },
} satisfies QueryScenario<typeof educationAppSchema>;
