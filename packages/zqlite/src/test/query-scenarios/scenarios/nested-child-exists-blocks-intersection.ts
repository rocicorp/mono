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
  name: 'nested child exists blocks sibling intersection',
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
    // Current behavior:
    //
    //   planner cost modeling reaches the nested child EXISTS and fails before
    //   the scenario can produce SQL.
    //
    // Desired plan:
    //
    //   teacher_access(teacher_id = currentTeacher) --------------.
    //                                                            +-- intersect assignment_id
    //   class(status = visible) -> assignment_to_class -----------'
    //
    // Intuition:
    //
    //   The current intersection helper only accepts plain child filters. A
    //   future planner could flatten this simple nested EXISTS into another
    //   child-driven key producer instead of rejecting the whole intersection.
    sql: [
      {
        table: 'teacher_assignment_access',
        sql: 'desired: teacher_access ids intersect visible-class assignment ids',
      },
    ],
  },
  knownFailure: {
    reason:
      'Nested EXISTS inside a child branch is outside the current scenario planner path.',
    current:
      'The scenario currently fails before SQL generation while the cost model plans the nested child relationship.',
    desired:
      'Flatten the nested class EXISTS into a child-driven key producer and intersect assignment ids.',
    currentError: 'Unexpected undefined value',
    engineIdea:
      'Represent nested relationship filters as composable key producers before lowering to IVM joins.',
  },
} satisfies QueryScenario<typeof educationAppSchema>;
