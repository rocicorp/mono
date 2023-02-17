import * as s from 'superstruct';
import {connectedMessageSchema} from './connected.js';
import {errorMessageSchema} from './error.js';
import {pokeMessageSchema} from './poke.js';
import {pongMessageSchema} from './pong.js';

export const downstreamSchema = s.union([
  connectedMessageSchema,
  pokeMessageSchema,
  errorMessageSchema,
  pongMessageSchema,
]);

export type Downstream = s.Infer<typeof downstreamSchema>;
