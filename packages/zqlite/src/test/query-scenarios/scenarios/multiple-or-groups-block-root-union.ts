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
const assignmentToClass = educationAppTables.assignment_to_class;
const assignmentToStudentRelationship = relationshipName(
  educationAppRelationships.assignment,
  'assignment_to_student',
);
const assignmentToClassRelationship = relationshipName(
  educationAppRelationships.assignment,
  'assignment_to_class',
);

export default {
  name: 'multiple OR groups still cannot use budgeted root union',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const assignmentToStudent = tables.assignment_to_student;
    const assignmentToClass = tables.assignment_to_class;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, i === 101 || i === 1_500 ? 1 : 2, null, i);
    }

    db.prepare(
      `INSERT INTO ${tableName(assignmentToStudent)} (${colName(assignmentToStudent, 'assignment_id')}, ${colName(assignmentToStudent, 'student_id')}, ${colName(assignmentToStudent, 'created_at')}) VALUES (?, ?, ?)`,
    ).run(102, 'student-1', 102);

    db.prepare(
      `INSERT INTO ${tableName(assignmentToClass)} (${colName(assignmentToClass, 'assignment_id')}, ${colName(assignmentToClass, 'class_id')}) VALUES (?, ?)`,
    ).run(101, 10);
  },
  query: builder =>
    builder[assignment.name]
      .where(({and, cmp, exists, or}) =>
        and(
          or(
            cmp(colName(assignment, 'teacher_id'), '=', 1),
            exists(assignmentToStudentRelationship, membership =>
              membership.where(
                colName(assignmentToStudent, 'student_id'),
                '=',
                'student-1',
              ),
            ),
          ),
          or(
            cmp(colName(assignment, 'id'), '=', 1_500),
            exists(assignmentToClassRelationship, assignmentClass =>
              assignmentClass.where(
                colName(assignmentToClass, 'class_id'),
                '=',
                10,
              ),
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
    //     (teacher_id = currentTeacher OR EXISTS membership(student))
    //     AND
    //     (id = pinnedAssignment OR EXISTS assignment_to_class(class))
    //   )
    //
    // Current plan:
    //
    //   assignment
    //     |-- evaluate first OR
    //     `-- evaluate second OR
    //
    // Desired plan:
    //
    //   small DNF expansion with a budget:
    //
    //     teacher_id branch AND pinned id branch
    //     teacher_id branch AND class branch
    //     membership branch AND pinned id branch
    //     membership branch AND class branch
    //
    // Intuition:
    //
    //   One OR group can be split today. Two independent OR groups would need a
    //   costed expansion budget so we do not accidentally explode the plan.
    sql: [
      {
        table: 'assignment',
        sql: 'desired: costed DNF root union for two small OR groups',
      },
    ],
  },
  knownFailure: {
    reason:
      'Root union currently handles one OR group, not multiple independent OR groups.',
    current:
      'The engine keeps one parent stream and evaluates the OR groups as filters.',
    desired:
      'Expand small OR groups into selective physical branches only when the branch count stays under a cost budget.',
    currentSQL: [
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE TRUE ORDER BY "created_at" desc, "id" asc',
      },
      {
        table: 'assignment_to_class',
        sql: 'SELECT "assignment_id","class_id" FROM "assignment_to_class" WHERE "assignment_id" = ? AND "class_id" = ? ORDER BY "assignment_id" asc, "class_id" asc',
        calls: 2001,
      },
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? AND TRUE ORDER BY "created_at" desc, "id" asc',
      },
    ],
    engineIdea:
      'Add a costed DNF planner alternative with a hard fanout cap and common-filter factoring.',
  },
} satisfies QueryScenario<typeof educationAppSchema>;
