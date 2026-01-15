import type * as v from '../../../../../../shared/src/valita.ts';
import {statusMessageSchema} from './status.ts';

/** At the moment, the only upstream messages are status messages.  */
export const changeSourceUpstreamSchema = statusMessageSchema;
export type ChangeSourceUpstream = v.Infer<typeof changeSourceUpstreamSchema>;
