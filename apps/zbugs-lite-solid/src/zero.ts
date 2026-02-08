// Import Zero config entirely from zbugs - no local schema
// This app is a Solid rendering layer on top of zbugs Zero infrastructure

export {
  schema,
  builder,
  ZERO_PROJECT_ID,
  ZERO_PROJECT_NAME,
} from '../../zbugs/shared/schema.ts';
export {mutators} from '../../zbugs/shared/mutators.ts';
export {queries} from '../../zbugs/shared/queries.ts';

// Re-export the Schema type for convenience
import type {schema} from '../../zbugs/shared/schema.ts';
export type Schema = typeof schema;
