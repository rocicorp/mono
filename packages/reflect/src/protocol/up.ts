import * as z from 'zod';
import {pingMessageSchema} from './ping.js';
import {pushMessageSchema} from './push.js';

export const upstreamSchema = z.union([pingMessageSchema, pushMessageSchema]);

export type Upstream = z.infer<typeof upstreamSchema>;
