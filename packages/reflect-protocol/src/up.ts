import * as v from '@badrap/valita';
import {pingMessageSchema} from './ping.js';
import {pullRequestMessageSchema} from './pull.js';
import {pushMessageSchema} from './push.js';

export const upstreamSchema = v.union(
  pingMessageSchema,
  pushMessageSchema,
  pullRequestMessageSchema,
);

export type Upstream = v.Infer<typeof upstreamSchema>;
