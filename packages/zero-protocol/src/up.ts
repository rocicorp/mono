import * as v from 'shared/src/valita.js';
import {pingMessageSchema} from './ping.js';
import {deleteClientsMessageSchema} from './delete-clients.js';
import {initConnectionMessageSchema} from './connect.js';
import {pullRequestMessageSchema} from './pull.js';
import {pushMessageSchema} from './push.js';
import {changeDesiredQueriesMessageSchema} from './change-desired-queries.js';

export const upstreamSchema = v.union(
  initConnectionMessageSchema,
  pingMessageSchema,
  deleteClientsMessageSchema,
  changeDesiredQueriesMessageSchema,
  pullRequestMessageSchema,
  pushMessageSchema,
);

export type Upstream = v.Infer<typeof upstreamSchema>;
