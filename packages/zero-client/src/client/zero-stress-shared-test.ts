import type {zeroStressSchema} from './zero-stress-schema-test.ts';

export type StressContext = {
  userId: string;
  role: 'admin' | 'user';
  workspaceId?: string;
  workspaceName?: string;
  fieldId?: string;
};

export type StressTransaction = {
  // include the entire giant schema
  $schema: typeof zeroStressSchema;
};
