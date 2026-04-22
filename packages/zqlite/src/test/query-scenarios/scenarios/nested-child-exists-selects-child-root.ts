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
const teacherAccess = educationAppTables.teacher_assignment_access;
const classroom = educationAppTables.class;
const teacherAccessRelationship = relationshipName(
  educationAppRelationships.assignment,
  'teacher_access',
);
const assignmentToClassRelationship = relationshipName(
  educationAppRelationships.assignment,
  'assignment_to_class',
);
const classRelationship = relationshipName(
  educationAppRelationships.assignment_to_class,
  'class',
);

export default {
  name: 'nested child exists selects child root',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const teacherAccess = tables.teacher_assignment_access;
    const assignmentToClass = tables.assignment_to_class;
    const classroom = tables.class;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, 2, null, i);
    }

    const teacherAccessStmt = db.prepare(
      `INSERT INTO ${tableName(teacherAccess)} (${colName(teacherAccess, 'assignment_id')}, ${colName(teacherAccess, 'teacher_id')}, ${colName(teacherAccess, 'access_kind')}) VALUES (?, ?, ?)`,
    );
    teacherAccessStmt.run(101, 1, 'direct');
    teacherAccessStmt.run(102, 1, 'class');
    teacherAccessStmt.run(103, 1, 'group');

    const classStmt = db.prepare(
      `INSERT INTO ${tableName(classroom)} (${colName(classroom, 'id')}, ${colName(classroom, 'status')}, ${colName(classroom, 'school_id')}) VALUES (?, ?, ?)`,
    );
    classStmt.run(10, 'visible', 1);
    classStmt.run(11, 'hidden', 1);

    const assignmentToClassStmt = db.prepare(
      `INSERT INTO ${tableName(assignmentToClass)} (${colName(assignmentToClass, 'assignment_id')}, ${colName(assignmentToClass, 'class_id')}) VALUES (?, ?)`,
    );
    assignmentToClassStmt.run(102, 10);
    assignmentToClassStmt.run(103, 11);
    assignmentToClassStmt.run(1_500, 10);
  },
  query: builder =>
    builder[assignment.name]
      .whereExists(teacherAccessRelationship, access =>
        access.where(colName(teacherAccess, 'teacher_id'), '=', 1),
      )
      .whereExists(assignmentToClassRelationship, assignmentClass =>
        assignmentClass.whereExists(classRelationship, classQuery =>
          classQuery.where(colName(classroom, 'status'), '=', 'visible'),
        ),
      )
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
  expectations: {
    // Submitted ZQL:
    //
    //   assignment
    //     .whereExists(teacher_access, teacher_id = currentTeacher)
    //     .whereExists(assignment_to_class,
    //       EXISTS class(status = visible)
    //     )
    //
    // Naive plan:
    //
    //   assignment
    //     |
    //     +-> probe teacher_access(teacher_id = currentTeacher)
    //     |
    //     `-> probe assignment_to_class
    //           |
    //           `-> probe class(status = visible)
    //
    // Optimized plan:
    //
    //   class(status = visible)
    //     |
    //     v
    //   assignment_to_class(class_id)
    //     |
    //     v
    //   assignment(id)
    //     |
    //     `-> probe teacher_access(teacher_id = currentTeacher)
    //
    // Intuition:
    //
    //   This nested EXISTS is supported. The planner follows the visible class
    //   branch first because it produces a tiny set of assignment ids, then it
    //   checks teacher access only for those candidates. This is not the sibling
    //   intersection helper. It is the general flip planner choosing the nested
    //   child path as the cheapest root.
    sql: [
      {
        table: 'class',
        sql: 'SELECT "id","status","school_id" FROM "class" WHERE "status" = ? ORDER BY "id" asc',
      },
      {
        table: 'assignment_to_class',
        sql: 'SELECT "assignment_id","class_id" FROM "assignment_to_class" WHERE "class_id" = ? ORDER BY "assignment_id" asc, "class_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? AND TRUE ORDER BY "created_at" desc, "id" asc',
        calls: 2,
      },
      {
        table: 'teacher_assignment_access',
        sql: 'SELECT "assignment_id","teacher_id","access_kind" FROM "teacher_assignment_access" WHERE "assignment_id" = ? AND "teacher_id" = ? ORDER BY "assignment_id" asc, "teacher_id" asc',
        calls: 3,
      },
    ],
    rows: [{id: 102, teacher_id: 2, archived_at: null, created_at: 102}],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
