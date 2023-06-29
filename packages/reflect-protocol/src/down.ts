import * as v from 'shared/src/valita.js';
import {connectedMessageSchema} from './connected.js';
import {errorMessageSchema} from './error.js';
import {pokeMessageSchema} from './poke.js';
import {pongMessageSchema} from './pong.js';
import {pullResponseMessageSchema} from './pull.js';

export const downstreamSchema = v.union(
  connectedMessageSchema,
  pokeMessageSchema,
  pullResponseMessageSchema,
  errorMessageSchema,
  pongMessageSchema,
);

export type Downstream = v.Infer<typeof downstreamSchema>;
