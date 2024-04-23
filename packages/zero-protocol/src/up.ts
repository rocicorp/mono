import * as v from 'shared/src/valita.js';
import {pingMessageSchema} from './ping.js';
import {deleteClientsMessageSchema} from './delete-clients.js';
import {initConnectionMessageSchema} from './connect.js';
import {pullRequestMessageSchema} from './pull.js';
import {changeDesiredQueriesMessageSchema} from './change-desired-queries.js';

export const upstreamSchema = v.union(
  initConnectionMessageSchema,
  pingMessageSchema,
  deleteClientsMessageSchema,
  changeDesiredQueriesMessageSchema,
  pullRequestMessageSchema,
);

export type Upstream = v.Infer<typeof upstreamSchema>;
