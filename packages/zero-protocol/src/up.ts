import * as v from '../../shared/src/valita.ts';
import {changeDesiredQueriesMessageSchema} from './change-desired-queries.ts';
import {closeConnectionMessageSchema} from './close-connection.ts';
import {initConnectionMessageSchema} from './connect.ts';
import {deleteClientsMessageSchema} from './delete-clients.ts';
import {inspectUpMessageSchema} from './inspect-up.ts';
import {pingMessageSchema} from './ping.ts';
import {pullRequestMessageSchema} from './pull.ts';
import {ackMutationResponsesMessageSchema, pushMessageSchema} from './push.ts';

export const upstreamSchema = v.union(
  initConnectionMessageSchema,
  pingMessageSchema,
  deleteClientsMessageSchema,
  changeDesiredQueriesMessageSchema,
  pullRequestMessageSchema,
  pushMessageSchema,
  closeConnectionMessageSchema,
  inspectUpMessageSchema,
  ackMutationResponsesMessageSchema,
);

export type Upstream = v.Infer<typeof upstreamSchema>;
