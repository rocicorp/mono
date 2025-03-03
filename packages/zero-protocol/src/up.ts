import * as v from '../../shared/src/valita.ts';
import {changeDesiredQueriesMessageSchema} from './change-desired-queries.ts';
import {closeConnectionMessageSchema} from './close-connection.ts';
import {initConnectionMessageSchema} from './connect.ts';
import {deleteClientsMessageSchema} from './delete-clients.ts';
import {pingMessageSchema} from './ping.ts';
import {pullRequestMessageSchema} from './pull.ts';
import {pushMessageSchema} from './push.ts';

export const upstreamSchema = v.union(
  initConnectionMessageSchema,
  pingMessageSchema,
  deleteClientsMessageSchema,
  changeDesiredQueriesMessageSchema,
  pullRequestMessageSchema,
  pushMessageSchema,
  closeConnectionMessageSchema,
);

export type Upstream = v.Infer<typeof upstreamSchema>;
