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
  name: 'root OR with limit still cannot use union roots',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const assignmentToStudent = tables.assignment_to_student;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, i % 100 === 0 ? 1 : 2, null, i);
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
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc')
      .limit(2),
  expectations: {
    // Submitted ZQL:
    //
    //   assignment
    //     .where(teacher_id = currentTeacher OR EXISTS membership(student))
    //     .orderBy(created_at desc)
    //     .limit(2)
    //
    // Current plan:
    //
    //   assignment ordered scan
    //     `-- filter teacher_id branch inside the stream
    //
    //   assignment_to_student(student = selectedStudent) -> parent
    //
    //   both paths then flow through the outer limit
    //
    // Desired plan:
    //
    //   assignment(teacher_id = currentTeacher) ------------------.
    //                                                             +-- union ids
    //   assignment_to_student(student = selectedStudent) -> parent -'
    //                                                             |
    //                                                             v
    //                                                global order, dedupe, limit
    //
    // Intuition:
    //
    //   The root-union shortcut currently stops when the whole result stream is
    //   observed by limit/start. A richer physical plan could union candidate
    //   ids, apply final ordering and limit once, then fetch only the winners.
    sql: [
      {
        table: 'assignment',
        sql: 'desired: parent teacher_id branch, child membership branch, then global limit',
      },
    ],
  },
  knownFailure: {
    reason: 'Root union currently refuses queries with a root limit.',
    current:
      'The engine keeps the older parent-rooted plan so the limit observes one ordered parent stream.',
    desired:
      'Union selective parent ids from both branches, dedupe and globally order them, then apply limit.',
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
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" IN (?,?,?) ORDER BY "created_at" desc, "id" asc',
      },
    ],
    engineIdea:
      'Introduce a key-union physical node that can feed a single Take/Skip after merge ordering.',
  },
} satisfies QueryScenario<typeof educationAppSchema>;
