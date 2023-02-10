import * as s from "superstruct";
import { connectedMessageSchema } from "./connected";
import { errorMessageSchema } from "./error";
import { pokeMessageSchema } from "./poke";
import { pongMessageSchema } from "./pong";

export const downstreamSchema = s.union([
  connectedMessageSchema,
  pokeMessageSchema,
  errorMessageSchema,
  pongMessageSchema,
]);

export type Downstream = s.Infer<typeof downstreamSchema>;
