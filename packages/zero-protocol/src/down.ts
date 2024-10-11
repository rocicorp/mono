import * as v from '../../shared/src/valita.js';
import {connectedMessageSchema} from './connect.js';
import {errorMessageSchema} from './error.js';
import {
  pokeEndMessageSchema,
  pokePartMessageSchema,
  pokeStartMessageSchema,
} from './poke.js';
import {pongMessageSchema} from './pong.js';
import {pullResponseMessageSchema} from './pull.js';
import {warmMessageSchema} from './warm.js';

export const downstreamSchema = v.union(
  connectedMessageSchema,
  warmMessageSchema,
  errorMessageSchema,
  pongMessageSchema,
  pokeStartMessageSchema,
  pokePartMessageSchema,
  pokeEndMessageSchema,
  pullResponseMessageSchema,
);

export type Downstream = v.Infer<typeof downstreamSchema>;
