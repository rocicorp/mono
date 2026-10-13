import * as v from '../../shared/src/valita.ts';
import {changeDesiredQueriesMessageSchema} from './change-desired-queries.ts';
import {closeConnectionMessageSchema} from './close-connection.ts';
import {initConnectionMessageSchema} from './connect.ts';
import {deleteClientsMessageSchema} from './delete-clients.ts';
import {
  inspectUpMessageSchema,
  unparsedInspectUpMessageSchema,
} from './inspect-up.ts';
import {pingMessageSchema} from './ping.ts';
import {pullRequestMessageSchema} from './pull.ts';
import {ackMutationResponsesMessageSchema, pushMessageSchema} from './push.ts';
import {updateAuthMessageSchema} from './update-auth.ts';

export const upstreamSchema = v.taggedTupleUnion(
  [
    initConnectionMessageSchema,
    pingMessageSchema,
    deleteClientsMessageSchema,
    changeDesiredQueriesMessageSchema,
    pullRequestMessageSchema,
    updateAuthMessageSchema,
    pushMessageSchema,
    closeConnectionMessageSchema,
    inspectUpMessageSchema,
    ackMutationResponsesMessageSchema,
  ],
  {open: true},
);

export const upstreamSchemaWithUnparsedAnalyzeQuery = v.taggedTupleUnion(
  [
    initConnectionMessageSchema,
    pingMessageSchema,
    deleteClientsMessageSchema,
    changeDesiredQueriesMessageSchema,
    pullRequestMessageSchema,
    updateAuthMessageSchema,
    pushMessageSchema,
    closeConnectionMessageSchema,
    unparsedInspectUpMessageSchema,
    ackMutationResponsesMessageSchema,
  ],
  {open: true},
);

// The messages this build knows. Parsing can also return a v.UnrecognizedTuple
// from a newer client; check with `isKnown`.
export type Upstream = Exclude<
  v.Infer<typeof upstreamSchema>,
  v.UnrecognizedTuple
>;
export type UpstreamWithUnparsedAnalyzeQuery = Exclude<
  v.Infer<typeof upstreamSchemaWithUnparsedAnalyzeQuery>,
  v.UnrecognizedTuple
>;
