import { z } from "zod";
import { connectedMessageSchema } from "./connected";
import { errorMessageSchema } from "./error";
import { pokeMessageSchema } from "./poke";
import { pongMessageSchema } from "./pong";

export const downstreamSchema = z.union([
  connectedMessageSchema,
  pokeMessageSchema,
  errorMessageSchema,
  pongMessageSchema,
]);

export type Downstream = z.infer<typeof downstreamSchema>;
