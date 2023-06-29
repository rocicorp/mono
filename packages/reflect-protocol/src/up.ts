import * as v from 'shared/src/valita.js';
import {pingMessageSchema} from './ping.js';
import {pullRequestMessageSchema} from './pull.js';
import {pushMessageSchema} from './push.js';

export const upstreamSchema = v.union(
  pingMessageSchema,
  pushMessageSchema,
  pullRequestMessageSchema,
);

export type Upstream = v.Infer<typeof upstreamSchema>;
