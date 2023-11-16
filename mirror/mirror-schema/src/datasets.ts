import * as v from 'shared/src/valita.js';
import {Dataset} from '../../cloudflare-api/src/dataset.js';

export const runningConnectionSeconds = new Dataset(
  'RunningConnectionSeconds',
  v.object({
    teamID: v.string(),
    appID: v.string(),
    roomID: v.string(),
    elapsed: v.number(),
    period: v.number(), // Note: "interval" is a reserved word in Analytics SQL. Using "period" instead.
  }),
);

export type RunningConnectionSecondsRow = v.Infer<
  typeof runningConnectionSeconds.output
>;

export const connectionLifetimes = new Dataset(
  'ConnectionLifetimes',
  v.object({
    teamID: v.string(),
    appID: v.string(),
    startTime: v.number(),
    endTime: v.number(),
  }),
);

export type ConnectionLifetimesRow = v.Infer<typeof connectionLifetimes.output>;

export const ALL_DATASETS = [
  runningConnectionSeconds.name,
  connectionLifetimes.name,
] as const;
