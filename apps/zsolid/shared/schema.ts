// Re-export schema, builder, and mutators from zbugs to use the exact same definitions
export {schema, builder} from '../../zbugs/shared/schema.ts';
export {mutators} from '../../zbugs/shared/mutators.ts';
// Re-export queries from zbugs (they're registered with the server)
export {queries} from '../../zbugs/shared/queries.ts';

import {schema} from '../../zbugs/shared/schema.ts';
import {definePermissions, ANYONE_CAN} from '@rocicorp/zero';

// Allow anyone to read all tables for this test app
export const permissions = definePermissions(schema, () => ({
  user: {row: {select: ANYONE_CAN}},
  project: {row: {select: ANYONE_CAN}},
  issue: {row: {select: ANYONE_CAN}},
  comment: {row: {select: ANYONE_CAN}},
  label: {row: {select: ANYONE_CAN}},
  issueLabel: {row: {select: ANYONE_CAN}},
  viewState: {row: {select: ANYONE_CAN}},
  emoji: {row: {select: ANYONE_CAN}},
  userPref: {row: {select: ANYONE_CAN}},
  issueNotifications: {row: {select: ANYONE_CAN}},
}));
