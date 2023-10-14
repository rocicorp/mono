import * as v from 'shared/src/valita.js';

export const workerNameSchema = v.union(
  v.literal('dispatcher'),
  v.literal('connections-reporter'),
);

export type WorkerName = v.Infer<typeof workerNameSchema>;

export const TAIL_WORKERS: readonly WorkerName[] = [
  'connections-reporter',
] as const;
