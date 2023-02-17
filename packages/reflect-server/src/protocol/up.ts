import * as s from 'superstruct';
import {pingMessageSchema} from './ping.js';
import {pushMessageSchema} from './push.js';

export const upstreamSchema = s.union([pushMessageSchema, pingMessageSchema]);

export type Upstream = s.Infer<typeof upstreamSchema>;
