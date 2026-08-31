import type {QueryScenario} from '../../query-scenario.ts';
import type {educationAppSchema} from '../education-app.ts';
import childDomainIntersection from './child-domain-intersection.ts';
import contradictoryParentDomain from './contradictory-parent-domain.ts';
import duplicateExistsOrDedupe from './duplicate-exists-or-dedupe.ts';
import explicitFlipFalse from './explicit-flip-false.ts';
import explicitFlipTrue from './explicit-flip-true.ts';
import impossibleExistsOrParentFilter from './impossible-exists-or-parent-filter.ts';
import mixedOrWithFalseAndMembership from './mixed-or-with-false-and-membership.ts';
import multipleOrGroupsBlockRootUnion from './multiple-or-groups-block-root-union.ts';
import nestedChildExistsSelectsChildRoot from './nested-child-exists-selects-child-root.ts';
import nonUniqueChildIdBlocksIntersection from './non-unique-child-id-blocks-intersection.ts';
import notExistsAntiJoinUnsupported from './not-exists-anti-join-unsupported.ts';
import orWithFalseBranch from './or-with-false-branch.ts';
import parentOrExistsUnionRoots from './parent-or-exists-union-roots.ts';
import permissionAndClassFilterIntersection from './permission-and-class-filter-intersection.ts';
import permissionSystemParentOrExistsUnion from './permission-system-parent-or-exists-union.ts';
import permissionSystemSiblingIntersection from './permission-system-sibling-intersection.ts';
import rootOrLimitBlocksUnion from './root-or-limit-blocks-union.ts';
import rootOrRelatedBlocksUnion from './root-or-related-blocks-union.ts';
import sameRelationshipAndIntersect from './same-relationship-and-intersect.ts';
import sameRelationshipOrMerge from './same-relationship-or-merge.ts';
import sampleAssignmentOrTeacherAccess from './sample-assignment-or-teacher-access.ts';
import sharedParentFilterOrFactor from './shared-parent-filter-or-factor.ts';
import singleExistsChildOr from './single-exists-child-or.ts';
import skewedCrossRelationshipDeclinesIntersection from './skewed-cross-relationship-declines-intersection.ts';
import studentAssignmentDirectClassGroupUnion from './student-assignment-direct-class-group-union.ts';
import studentMembershipMixedOr from './student-membership-mixed-or.ts';
import studentMembershipSimpleExists from './student-membership-simple-exists.ts';
import teacherFilterOrIn from './teacher-filter-or-in.ts';
import teacherFilterWithMembership from './teacher-filter-with-membership.ts';
import twoStudentMembershipOr from './two-student-membership-or.ts';
import wideFlatPermissionOrBudget from './wide-flat-permission-or-budget.ts';
import wideOrUnionScalabilityWatchpoint from './wide-or-union-scalability-watchpoint.ts';

export default [
  studentMembershipMixedOr,
  studentMembershipSimpleExists,
  teacherFilterOrIn,
  contradictoryParentDomain,
  childDomainIntersection,
  teacherFilterWithMembership,
  explicitFlipFalse,
  explicitFlipTrue,
  impossibleExistsOrParentFilter,
  orWithFalseBranch,
  mixedOrWithFalseAndMembership,
  twoStudentMembershipOr,
  singleExistsChildOr,
  sameRelationshipOrMerge,
  parentOrExistsUnionRoots,
  sampleAssignmentOrTeacherAccess,
  sharedParentFilterOrFactor,
  sameRelationshipAndIntersect,
  duplicateExistsOrDedupe,
  permissionSystemParentOrExistsUnion,
  permissionSystemSiblingIntersection,
  permissionAndClassFilterIntersection,
  skewedCrossRelationshipDeclinesIntersection,
  studentAssignmentDirectClassGroupUnion,
  rootOrLimitBlocksUnion,
  rootOrRelatedBlocksUnion,
  nestedChildExistsSelectsChildRoot,
  nonUniqueChildIdBlocksIntersection,
  multipleOrGroupsBlockRootUnion,
  wideFlatPermissionOrBudget,
  wideOrUnionScalabilityWatchpoint,
  notExistsAntiJoinUnsupported,
] satisfies readonly QueryScenario<typeof educationAppSchema>[];
