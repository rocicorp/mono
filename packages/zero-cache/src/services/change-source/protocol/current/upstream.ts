import * as v from '../../../../../../shared/src/valita.js';
import {statusMessageSchema} from './status.js';

/** At the moment, the only upstream messages are status messages.  */
export const changeSourceUpstreamSchema = statusMessageSchema;
export type ChangeSourceUpstream = v.Infer<typeof changeSourceUpstreamSchema>;
