import * as s from "superstruct";
import { pingMessageSchema } from "./ping";
import { pushMessageSchema } from "./push";

export const upstreamSchema = s.union([pushMessageSchema, pingMessageSchema]);

export type Upstream = s.Infer<typeof upstreamSchema>;
