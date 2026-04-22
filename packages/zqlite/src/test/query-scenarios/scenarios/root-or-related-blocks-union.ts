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
  name: 'root OR with related rows still cannot use union roots',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const assignmentToStudent = tables.assignment_to_student;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, i === 4 ? 1 : 2, null, i);
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
      .where(({cmp, exists, or}) =>
        or(
          cmp(colName(assignment, 'teacher_id'), '=', 1),
          exists(assignmentToStudentRelationship, q =>
            q.where(
              colName(assignmentToStudent, 'student_id'),
              '=',
              'student-1',
            ),
          ),
        ),
      )
      .related(assignmentToStudentRelationship)
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
  expectations: {
    // Submitted ZQL:
    //
    //   assignment
    //     .where(teacher_id = currentTeacher OR EXISTS membership(student))
    //     .related(assignment_to_student)
    //
    // Current plan:
    //
    //   assignment ordered scan
    //     |-- filter teacher_id branch inside the stream
    //     `-- hydrate assignment_to_student rows
    //
    //   assignment_to_student(student = selectedStudent) -> parent
    //     `-- hydrate assignment_to_student rows
    //
    // Desired plan:
    //
    //   assignment(teacher_id = currentTeacher) ------------------.
    //                                                             +-- union ids
    //   assignment_to_student(student = selectedStudent) -> parent -'
    //                                                             |
    //                                                             v
    //                                              hydrate related rows once
    //
    // Intuition:
    //
    //   Relationship hydration observes parent rows, but it does not need to
    //   decide which parent ids qualify. A future plan could find the parent id
    //   set first, then attach related rows after the union.
    sql: [
      {
        table: 'assignment',
        sql: 'desired: union parent ids first, then hydrate assignment_to_student',
      },
    ],
  },
  knownFailure: {
    reason:
      'Root union currently refuses queries that hydrate root related rows.',
    current:
      'The engine keeps the older parent-rooted stream so related rows stay attached to one parent pipeline.',
    desired:
      'Compute the unioned parent id set first, then run related-row hydration against the final parent stream.',
    currentSQL: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? ORDER BY "created_at" desc, "id" asc',
        calls: 3,
      },
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "assignment_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
        calls: 4,
      },
    ],
    engineIdea:
      'Separate condition-only branch planning from relationship hydration so set operators only merge parent rows.',
  },
} satisfies QueryScenario<typeof educationAppSchema>;
