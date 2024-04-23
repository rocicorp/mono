import * as v from 'shared/src/valita.js';
import {pingMessageSchema} from './ping.js';
import {deleteClientsMessageSchema} from './delete-clients.js';

export const upstreamSchema = v.union(
  pingMessageSchema,
  deleteClientsMessageSchema,
);

export type Upstream = v.Infer<typeof upstreamSchema>;
