import type {QueryScenario} from '../../query-scenario.ts';
import type {educationAppSchema} from '../education-app.ts';
import explicitFlipFalse from './explicit-flip-false.ts';
import explicitFlipTrue from './explicit-flip-true.ts';
import mixedOrWithFalseAndMembership from './mixed-or-with-false-and-membership.ts';
import orWithFalseBranch from './or-with-false-branch.ts';
import studentMembershipMixedOr from './student-membership-mixed-or.ts';
import studentMembershipSimpleExists from './student-membership-simple-exists.ts';
import teacherFilterWithMembership from './teacher-filter-with-membership.ts';
import twoStudentMembershipOr from './two-student-membership-or.ts';

export default [
  studentMembershipMixedOr,
  studentMembershipSimpleExists,
  teacherFilterWithMembership,
  explicitFlipFalse,
  explicitFlipTrue,
  orWithFalseBranch,
  mixedOrWithFalseAndMembership,
  twoStudentMembershipOr,
] satisfies readonly QueryScenario<typeof educationAppSchema>[];
