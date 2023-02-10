import { z } from "zod";
import { pingMessageSchema } from "./ping";
import { pushMessageSchema } from "./push";

export const upstreamSchema = z.union([pushMessageSchema, pingMessageSchema]);

export type Upstream = z.infer<typeof upstreamSchema>;
