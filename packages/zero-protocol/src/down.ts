import * as v from '../../shared/src/valita.ts';
import {connectedMessageSchema} from './connect.ts';
import {deleteClientsMessageSchema} from './delete-clients.ts';
import {errorMessageSchema} from './error.ts';
import {
  pokeEndMessageSchema,
  pokePartMessageSchema,
  pokeStartMessageSchema,
} from './poke.ts';
import {pongMessageSchema} from './pong.ts';
import {pullResponseMessageSchema} from './pull.ts';
import {pushResponseMessageSchema} from './push.ts';
import {warmMessageSchema} from './warm.ts';

export const downstreamSchema = v.union(
  connectedMessageSchema,
  warmMessageSchema,
  errorMessageSchema,
  pongMessageSchema,
  pokeStartMessageSchema,
  pokePartMessageSchema,
  pokeEndMessageSchema,
  pullResponseMessageSchema,
  deleteClientsMessageSchema,
  pushResponseMessageSchema,
);

export type Downstream = v.Infer<typeof downstreamSchema>;
