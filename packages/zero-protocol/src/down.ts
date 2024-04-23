import * as v from 'shared/src/valita.js';
import {connectedMessageSchema} from './connect.js';
import {errorMessageSchema} from './error.js';
import {
  pokeEndMessageSchema,
  pokePartMessageSchema,
  pokeStartMessageSchema,
} from './poke.js';
import {pongMessageSchema} from './pong.js';

export const downstreamSchema = v.union(
  connectedMessageSchema,
  pokeStartMessageSchema,
  pokePartMessageSchema,
  pokeEndMessageSchema,
  errorMessageSchema,
  pongMessageSchema,
);

export type Downstream = v.Infer<typeof downstreamSchema>;
