import {z} from 'zod';
import {connectedMessageSchema} from './connected.js';
import {errorMessageSchema} from './error.js';
import {pokeMessageSchema} from './poke.js';
import {pongMessageSchema} from './pong.js';

export const downstreamSchema = z.union([
  connectedMessageSchema,
  pokeMessageSchema,
  errorMessageSchema,
  pongMessageSchema,
]);

export type Downstream = z.infer<typeof downstreamSchema>;
