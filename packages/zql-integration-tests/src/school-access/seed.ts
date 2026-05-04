/**
 * Generates a deterministic dataset that mirrors the customer's profile:
 *   - 1 district, 1 school
 *   - 32 teachers at the school (31 regular + 1 school-administrator)
 *   - 29 classes, 29 teacher_to_class rows (1:1 teacher to class for the
 *     first 29 teachers; the admin and teachers 30..31 don't teach a class)
 *   - 564 students, 1128 student_class_membership rows (2 classes per student)
 *   - 1128 teacher_student_access rows (admin sees every (student, class)
 *     membership at their school)
 *
 * The user being authenticated is the school administrator.
 */

export const USER_ID = 'user-admin-1';

export const SEED = {
  numTeachers: 32,
  numClasses: 29,
  numStudents: 564,
  membershipsPerStudent: 2,
  adminTeacherId: 9999,
  districtId: 1,
  schoolId: 1,
};

export const DDL = /* sql */ `
CREATE TABLE district (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE school (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  district_id INTEGER NOT NULL
);
CREATE INDEX school_district_id_idx ON school (district_id);

CREATE TABLE teacher (
  id INTEGER PRIMARY KEY,
  user_id TEXT NOT NULL,
  role TEXT NOT NULL,
  school_id INTEGER NOT NULL
);
CREATE UNIQUE INDEX teacher_user_id_unique ON teacher (user_id);
CREATE INDEX teacher_school_id_idx ON teacher (school_id);

CREATE TABLE teacher_to_co_teacher (
  id INTEGER PRIMARY KEY,
  from_teacher_id INTEGER NOT NULL,
  to_teacher_id INTEGER NOT NULL
);
CREATE INDEX teacher_to_co_teacher_from_idx ON teacher_to_co_teacher (from_teacher_id);
CREATE INDEX teacher_to_co_teacher_to_idx ON teacher_to_co_teacher (to_teacher_id);

CREATE TABLE class (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  school_id INTEGER NOT NULL
);
CREATE INDEX class_school_id_idx ON class (school_id);

CREATE TABLE teacher_to_class (
  id INTEGER PRIMARY KEY,
  teacher_id INTEGER NOT NULL,
  class_id INTEGER NOT NULL
);
CREATE INDEX teacher_to_class_teacher_id_idx ON teacher_to_class (teacher_id);
CREATE INDEX teacher_to_class_class_id_idx ON teacher_to_class (class_id);

CREATE TABLE student (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE student_class_membership (
  id INTEGER PRIMARY KEY,
  student_id INTEGER NOT NULL,
  class_id INTEGER NOT NULL
);
CREATE INDEX student_class_membership_student_id_idx ON student_class_membership (student_id);
CREATE INDEX student_class_membership_class_id_idx ON student_class_membership (class_id);

CREATE TABLE teacher_student_access (
  id INTEGER PRIMARY KEY,
  teacher_id INTEGER NOT NULL,
  student_id INTEGER NOT NULL
);
CREATE INDEX teacher_student_access_teacher_id_idx ON teacher_student_access (teacher_id);
CREATE INDEX teacher_student_access_student_id_idx ON teacher_student_access (student_id);
`;

export function generateInserts(): string {
  const lines: string[] = [];
  const {
    numTeachers,
    numClasses,
    numStudents,
    membershipsPerStudent,
    adminTeacherId,
    districtId,
    schoolId,
  } = SEED;

  lines.push(`INSERT INTO district VALUES (${districtId}, 'District 1');`);
  lines.push(
    `INSERT INTO school VALUES (${schoolId}, 'School 1', ${districtId});`,
  );

  // 31 regular teachers
  const regularTeachers = numTeachers - 1;
  const teacherValues: string[] = [];
  for (let i = 1; i <= regularTeachers; i++) {
    teacherValues.push(`(${i}, 'teacher-user-${i}', 'teacher', ${schoolId})`);
  }
  // 1 admin (the requester)
  teacherValues.push(
    `(${adminTeacherId}, '${USER_ID}', 'school-administrator', ${schoolId})`,
  );
  lines.push(
    `INSERT INTO teacher (id, user_id, role, school_id) VALUES ${teacherValues.join(
      ',\n  ',
    )};`,
  );

  // 29 classes
  const classValues: string[] = [];
  for (let i = 1; i <= numClasses; i++) {
    classValues.push(`(${i}, 'Class ${i}', ${schoolId})`);
  }
  lines.push(
    `INSERT INTO class (id, name, school_id) VALUES ${classValues.join(
      ',\n  ',
    )};`,
  );

  // 1 teacher_to_class per regular teacher i in [1..numClasses]
  const tcValues: string[] = [];
  for (let i = 1; i <= numClasses; i++) {
    tcValues.push(`(${i}, ${i}, ${i})`);
  }
  lines.push(
    `INSERT INTO teacher_to_class (id, teacher_id, class_id) VALUES ${tcValues.join(
      ',\n  ',
    )};`,
  );

  // students
  const studentValues: string[] = [];
  for (let s = 1; s <= numStudents; s++) {
    studentValues.push(`(${s}, 'Student ${s}')`);
  }
  lines.push(
    `INSERT INTO student (id, name) VALUES ${studentValues.join(',\n  ')};`,
  );

  // memberships: each student in `membershipsPerStudent` classes
  const membershipValues: string[] = [];
  let membershipId = 1;
  for (let s = 1; s <= numStudents; s++) {
    for (let m = 0; m < membershipsPerStudent; m++) {
      // Deterministic spread across classes
      const classId = ((s + m * 7) % numClasses) + 1;
      membershipValues.push(`(${membershipId++}, ${s}, ${classId})`);
    }
  }
  lines.push(
    `INSERT INTO student_class_membership (id, student_id, class_id) VALUES ${membershipValues.join(
      ',\n  ',
    )};`,
  );

  // teacher_student_access for the admin: 1 row per membership.
  // Admin sees every student-class pair at the school.
  const accessValues: string[] = [];
  let accessId = 1;
  for (let s = 1; s <= numStudents; s++) {
    for (let m = 0; m < membershipsPerStudent; m++) {
      accessValues.push(`(${accessId++}, ${adminTeacherId}, ${s})`);
    }
  }
  lines.push(
    `INSERT INTO teacher_student_access (id, teacher_id, student_id) VALUES ${accessValues.join(
      ',\n  ',
    )};`,
  );

  return lines.join('\n');
}

export function generatePgContent(): string {
  return `${DDL}\n${generateInserts()}\n`;
}
