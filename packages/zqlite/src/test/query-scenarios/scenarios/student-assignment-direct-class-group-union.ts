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
const assignmentToGroup = educationAppTables.assignment_to_group;

const assignmentToStudentRelationship = relationshipName(
  educationAppRelationships.assignment,
  'assignment_to_student',
);
const assignmentToClassRelationship = relationshipName(
  educationAppRelationships.assignment,
  'assignment_to_class',
);
const assignmentToGroupRelationship = relationshipName(
  educationAppRelationships.assignment,
  'assignment_to_group',
);

export default {
  name: 'assignment audience filter unions direct student class and group branches',
  schema: educationAppSchema,
  seed: db => {
    const tables = createEducationAppTables(db);
    const assignment = tables.assignment;
    const assignmentToStudent = tables.assignment_to_student;
    const assignmentToClass = tables.assignment_to_class;
    const assignmentToGroup = tables.assignment_to_group;
    const studentGroup = tables.student_group;

    const assignmentStmt = db.prepare(
      `INSERT INTO ${tableName(assignment)} (${colName(assignment, 'id')}, ${colName(assignment, 'teacher_id')}, ${colName(assignment, 'archived_at')}, ${colName(assignment, 'created_at')}) VALUES (?, ?, ?, ?)`,
    );
    for (let i = 1; i <= 2_000; i++) {
      assignmentStmt.run(i, 2, null, i);
    }

    db.prepare(
      `INSERT INTO ${tableName(assignmentToStudent)} (${colName(assignmentToStudent, 'assignment_id')}, ${colName(assignmentToStudent, 'student_id')}, ${colName(assignmentToStudent, 'created_at')}) VALUES (?, ?, ?)`,
    ).run(101, 'student-1', 101);

    db.prepare(
      `INSERT INTO ${tableName(educationAppTables.class)} (${colName(educationAppTables.class, 'id')}, ${colName(educationAppTables.class, 'status')}, ${colName(educationAppTables.class, 'school_id')}) VALUES (?, ?, ?)`,
    ).run(10, 'visible', 1);
    db.prepare(
      `INSERT INTO ${tableName(assignmentToClass)} (${colName(assignmentToClass, 'assignment_id')}, ${colName(assignmentToClass, 'class_id')}) VALUES (?, ?)`,
    ).run(102, 10);

    db.prepare(
      `INSERT INTO ${tableName(studentGroup)} (${colName(studentGroup, 'id')}, ${colName(studentGroup, 'name')}) VALUES (?, ?)`,
    ).run(20, 'group-a');
    db.prepare(
      `INSERT INTO ${tableName(assignmentToGroup)} (${colName(assignmentToGroup, 'assignment_id')}, ${colName(assignmentToGroup, 'group_id')}) VALUES (?, ?)`,
    ).run(103, 20);
  },
  query: builder =>
    builder[assignment.name]
      .where(({exists, or}) =>
        or(
          exists(assignmentToStudentRelationship, assignmentStudent =>
            assignmentStudent.where(
              colName(assignmentToStudent, 'student_id'),
              '=',
              'student-1',
            ),
          ),
          exists(assignmentToClassRelationship, assignmentClass =>
            assignmentClass.where(
              colName(assignmentToClass, 'class_id'),
              '=',
              10,
            ),
          ),
          exists(assignmentToGroupRelationship, assignmentGroup =>
            assignmentGroup.where(
              colName(assignmentToGroup, 'group_id'),
              '=',
              20,
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
    //     EXISTS assignment_to_student(student_id = selectedStudent)
    //     OR EXISTS assignment_to_class(class_id = selectedClass)
    //     OR EXISTS assignment_to_group(group_id = selectedGroup)
    //   )
    //
    // Naive plan:
    //
    //   assignment
    //     |-- probe direct student assignments
    //     |-- probe class assignments
    //     `-- probe group assignments
    //
    // Desired plan:
    //
    //   assignment_to_student(student_id = selectedStudent) --------.
    //                                                             |
    //   assignment_to_class(class_id = selectedClass)              +-- union assignment_id
    //                                                             |
    //   assignment_to_group(group_id = selectedGroup) -------------'
    //       `-- fetch assignment by assignment_id
    //
    // Intuition:
    //
    //   Assignment creation and filtering often let users mix individual
    //   students, classes, and groups. Each branch is selective on its own,
    //   and the planner should union those assignment ids before parent fetch.
    sql: [
      {
        table: 'assignment_to_student',
        sql: 'SELECT "assignment_id","student_id","created_at" FROM "assignment_to_student" WHERE "student_id" = ? ORDER BY "assignment_id" asc, "student_id" asc',
      },
      {
        table: 'assignment',
        sql: 'SELECT "id","teacher_id","archived_at","created_at" FROM "assignment" WHERE "id" IN (?) ORDER BY "created_at" desc, "id" asc',
        calls: 3,
      },
      {
        table: 'assignment_to_class',
        sql: 'SELECT "assignment_id","class_id" FROM "assignment_to_class" WHERE "class_id" = ? ORDER BY "assignment_id" asc, "class_id" asc',
      },
      {
        table: 'assignment_to_group',
        sql: 'SELECT "assignment_id","group_id" FROM "assignment_to_group" WHERE "group_id" = ? ORDER BY "assignment_id" asc, "group_id" asc',
      },
    ],
    rows: [
      {id: 103, teacher_id: 2, archived_at: null, created_at: 103},
      {id: 102, teacher_id: 2, archived_at: null, created_at: 102},
      {id: 101, teacher_id: 2, archived_at: null, created_at: 101},
    ],
  },
} satisfies QueryScenario<typeof educationAppSchema>;
