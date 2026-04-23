import type {educationAppSchema} from '../education-app.ts';
import type {QueryScenario} from '../scenario.ts';
import studentMembershipMixedOr from './student-membership-mixed-or.ts';
import studentMembershipSimpleExists from './student-membership-simple-exists.ts';

export default [
  studentMembershipSimpleExists,
  studentMembershipMixedOr,
] satisfies readonly QueryScenario<typeof educationAppSchema>[];
