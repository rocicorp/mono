import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';

const district = table('district')
  .columns({
    id: number(),
    name: string(),
  })
  .primaryKey('id');

const school = table('school')
  .columns({
    id: number(),
    name: string(),
    districtId: number().from('district_id'),
  })
  .primaryKey('id');

const teacher = table('teacher')
  .columns({
    id: number(),
    userId: string().from('user_id'),
    role: string(),
    schoolId: number().from('school_id'),
  })
  .primaryKey('id');

const teacherToCoTeacher = table('teacherToCoTeacher')
  .from('teacher_to_co_teacher')
  .columns({
    id: number(),
    fromTeacherId: number().from('from_teacher_id'),
    toTeacherId: number().from('to_teacher_id'),
  })
  .primaryKey('id');

const klass = table('class')
  .columns({
    id: number(),
    name: string(),
    schoolId: number().from('school_id'),
  })
  .primaryKey('id');

const teacherToClass = table('teacherToClass')
  .from('teacher_to_class')
  .columns({
    id: number(),
    teacherId: number().from('teacher_id'),
    classId: number().from('class_id'),
  })
  .primaryKey('id');

const student = table('student')
  .columns({
    id: number(),
    name: string(),
  })
  .primaryKey('id');

const studentClassMembership = table('studentClassMembership')
  .from('student_class_membership')
  .columns({
    id: number(),
    studentId: number().from('student_id'),
    classId: number().from('class_id'),
  })
  .primaryKey('id');

const teacherStudentAccess = table('teacherStudentAccess')
  .from('teacher_student_access')
  .columns({
    id: number(),
    teacherId: number().from('teacher_id'),
    studentId: number().from('student_id'),
  })
  .primaryKey('id');

const districtRelationships = relationships(district, ({many}) => ({
  schools: many({
    sourceField: ['id'],
    destField: ['districtId'],
    destSchema: school,
  }),
}));

const schoolRelationships = relationships(school, ({one, many}) => ({
  group: one({
    sourceField: ['districtId'],
    destField: ['id'],
    destSchema: district,
  }),
  teachers: many({
    sourceField: ['id'],
    destField: ['schoolId'],
    destSchema: teacher,
  }),
  classes: many({
    sourceField: ['id'],
    destField: ['schoolId'],
    destSchema: klass,
  }),
}));

const teacherRelationships = relationships(teacher, ({one, many}) => ({
  school: one({
    sourceField: ['schoolId'],
    destField: ['id'],
    destSchema: school,
  }),
  coTeacherGrants: many({
    sourceField: ['id'],
    destField: ['fromTeacherId'],
    destSchema: teacherToCoTeacher,
  }),
  classAssignments: many({
    sourceField: ['id'],
    destField: ['teacherId'],
    destSchema: teacherToClass,
  }),
  studentAccesses: many({
    sourceField: ['id'],
    destField: ['teacherId'],
    destSchema: teacherStudentAccess,
  }),
}));

const teacherToCoTeacherRelationships = relationships(
  teacherToCoTeacher,
  ({one}) => ({
    fromTeacher: one({
      sourceField: ['fromTeacherId'],
      destField: ['id'],
      destSchema: teacher,
    }),
    toTeacher: one({
      sourceField: ['toTeacherId'],
      destField: ['id'],
      destSchema: teacher,
    }),
  }),
);

const klassRelationships = relationships(klass, ({one, many}) => ({
  school: one({
    sourceField: ['schoolId'],
    destField: ['id'],
    destSchema: school,
  }),
  teachers: many({
    sourceField: ['id'],
    destField: ['classId'],
    destSchema: teacherToClass,
  }),
  memberships: many({
    sourceField: ['id'],
    destField: ['classId'],
    destSchema: studentClassMembership,
  }),
}));

const teacherToClassRelationships = relationships(teacherToClass, ({one}) => ({
  teacher: one({
    sourceField: ['teacherId'],
    destField: ['id'],
    destSchema: teacher,
  }),
  class: one({
    sourceField: ['classId'],
    destField: ['id'],
    destSchema: klass,
  }),
}));

const studentRelationships = relationships(student, ({many}) => ({
  classes: many({
    sourceField: ['id'],
    destField: ['studentId'],
    destSchema: studentClassMembership,
  }),
  teacherAccess: many({
    sourceField: ['id'],
    destField: ['studentId'],
    destSchema: teacherStudentAccess,
  }),
}));

const studentClassMembershipRelationships = relationships(
  studentClassMembership,
  ({one}) => ({
    student: one({
      sourceField: ['studentId'],
      destField: ['id'],
      destSchema: student,
    }),
    class: one({
      sourceField: ['classId'],
      destField: ['id'],
      destSchema: klass,
    }),
  }),
);

const teacherStudentAccessRelationships = relationships(
  teacherStudentAccess,
  ({one}) => ({
    teacher: one({
      sourceField: ['teacherId'],
      destField: ['id'],
      destSchema: teacher,
    }),
    student: one({
      sourceField: ['studentId'],
      destField: ['id'],
      destSchema: student,
    }),
  }),
);

export const schema = createSchema({
  tables: [
    district,
    school,
    teacher,
    teacherToCoTeacher,
    klass,
    teacherToClass,
    student,
    studentClassMembership,
    teacherStudentAccess,
  ],
  relationships: [
    districtRelationships,
    schoolRelationships,
    teacherRelationships,
    teacherToCoTeacherRelationships,
    klassRelationships,
    teacherToClassRelationships,
    studentRelationships,
    studentClassMembershipRelationships,
    teacherStudentAccessRelationships,
  ],
});

export const builder = createBuilder(schema);
