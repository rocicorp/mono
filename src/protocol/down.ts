import {z} from 'zod';
import {connectedMessageSchema} from './connected.js';
import {errorMessageSchema} from './error.js';
import {pokeSetMessageSchema} from './poke-set.js';
import {pokeMessageSchema} from './poke.js';
import {pongMessageSchema} from './pong.js';

export const downstreamSchema = z.union([
  connectedMessageSchema,
  pokeMessageSchema,
  errorMessageSchema,
  pongMessageSchema,
  pokeSetMessageSchema,
]);

export type Downstream = z.infer<typeof downstreamSchema>;
