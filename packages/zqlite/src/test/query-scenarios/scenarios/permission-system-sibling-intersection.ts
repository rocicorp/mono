import {
  markAllExistsAsPermissions,
  type QueryScenario,
} from '../../query-scenario.ts';
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
const assignmentToClass = educationAppTables.assignment_to_class;
const teacherAccessRelationship = relationshipName(
  educationAppRelationships.assignment,
  'teacher_access',
);
const assignmentToClassRelationship = relationshipName(
  educationAppRelationships.assignment,
  'assignment_to_class',
);

export default {
  name: 'permission system sibling exists can intersect child keys',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const teacherAccess = tables.teacher_assignment_access;
    const assignmentToClass = tables.assignment_to_class;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, 2, i === 103 ? 'archived' : null, i);
    }

    const teacherAccessStmt = db.prepare(
      `INSERT INTO ${tableName(teacherAccess)} (${colName(teacherAccess, 'assignment_id')}, ${colName(teacherAccess, 'teacher_id')}, ${colName(teacherAccess, 'access_kind')}) VALUES (?, ?, ?)`,
    );
    teacherAccessStmt.run(101, 1, 'direct');
    teacherAccessStmt.run(102, 1, 'class');
    teacherAccessStmt.run(103, 1, 'group');
    teacherAccessStmt.run(104, 1, 'direct');

    const assignmentToClassStmt = db.prepare(
      `INSERT INTO ${tableName(assignmentToClass)} (${colName(assignmentToClass, 'assignment_id')}, ${colName(assignmentToClass, 'class_id')}) VALUES (?, ?)`,
    );
    assignmentToClassStmt.run(102, 10);
    assignmentToClassStmt.run(103, 10);
    assignmentToClassStmt.run(1_500, 10);
  },
  query: builder =>
    builder[assignment.name]
      .where(colName(assignment, 'archived_at'), 'IS', null)
      .whereExists(teacherAccessRelationship, access =>
        access.where(colName(teacherAccess, 'teacher_id'), '=', 1),
      )
      .whereExists(assignmentToClassRelationship, assignmentClass =>
        assignmentClass.where(colName(assignmentToClass, 'class_id'), '=', 10),
      )
      .orderBy(colName(assignment, 'created_at'), 'desc')
      .orderBy(colName(assignment, 'id'), 'asc'),
  transformAST: markAllExistsAsPermissions,
  expectations: {
    // Submitted ZQL:
    //
    //   assignment
    //     .where(archived_at IS null)
    //     .whereExists(permission_access, teacher_id = currentTeacher)
    //     .whereExists(permission_class, class_id = selectedClass)
    //
    // Why this version is safe:
    //
    //   Both child tables are permission evidence. They decide which parent
    //   rows are visible, but their own rows are not hydrated to the client.
    //
    // Optimized plan:
    //
    //   permission_access(teacher_id = currentTeacher) --.
    //                                                      +-- intersect assignment ids
    //   permission_class(class_id = selectedClass) -------'
    //                                                      |
    //                                                      v
    //                                              assignment(id, archived_at)
    //
    // Intuition:
    //
    //   Intersecting ids before fetching parents is safe when the child rows
    //   are private evidence rather than synced helper state.
    sql: [
      {
        table: 'assignment_to_class',
        sql: 'SELECT "assignment_id","class_id" FROM "assignment_to_class" WHERE "class_id" = ? ORDER BY "assignment_id" asc, "class_id" asc',
      },
      {
        table: 'teacher_assignment_access',
        sql: 'SELECT "assignment_id","teacher_id","access_kind" FROM "teacher_assignment_access" WHERE "teacher_id" = ? ORDER BY "assignment_id" asc, "teacher_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" = ? AND "archived_at" IS ? ORDER BY "created_at" desc, "id" asc',
        calls: 2,
      },
    ],
    rows: [{id: 102, teacher_id: 2, archived_at: null, created_at: 102}],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
