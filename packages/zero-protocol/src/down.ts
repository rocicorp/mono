import * as v from '../../shared/src/valita.ts';
import {connectedMessageSchema} from './connect.ts';
import {errorMessageSchema} from './error.ts';
import {
  pokeEndMessageSchema,
  pokePartMessageSchema,
  pokeStartMessageSchema,
} from './poke.ts';
import {pongMessageSchema} from './pong.ts';
import {pullResponseMessageSchema} from './pull.ts';
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
);

export type Downstream = v.Infer<typeof downstreamSchema>;
