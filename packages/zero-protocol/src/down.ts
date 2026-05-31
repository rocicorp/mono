import * as v from '../../shared/src/valita.ts';
import {connectedMessageSchema} from './connect.ts';
import {transformErrorMessageSchema} from './custom-queries.ts';
import {deleteClientsMessageSchema} from './delete-clients.ts';
import {errorMessageSchema} from './error.ts';
import {inspectDownMessageSchema} from './inspect-down.ts';
import {
  pokeEndMessageSchema,
  pokePartMessageSchema,
  pokeStartMessageSchema,
} from './poke.ts';
import {pongMessageSchema} from './pong.ts';
import {pullResponseMessageSchema} from './pull.ts';
import {pushResponseMessageSchema} from './push.ts';

export const downstreamSchema = v.union(
  connectedMessageSchema,
  errorMessageSchema,
  pongMessageSchema,
  pokeStartMessageSchema,
  pokePartMessageSchema,
  pokeEndMessageSchema,
  pullResponseMessageSchema,
  deleteClientsMessageSchema,
  pushResponseMessageSchema,
  inspectDownMessageSchema,
  transformErrorMessageSchema,
);

export type Downstream = v.Infer<typeof downstreamSchema>;
