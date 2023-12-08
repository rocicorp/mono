import type * as v from 'shared/src/valita.js';
import {pushBodySchema} from './push.js';

export const disconnectSchema = pushBodySchema;

export type Disconnect = v.Infer<typeof disconnectSchema>;
