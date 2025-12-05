/**
 * Control plane messages communicate non-content related signals between a
 * ChangeSource and ChangeStreamer. These are not forwarded to subscribers
 * of the ChangeStreamer.
 */
import * as v from '../../../../../../shared/src/valita.ts';
import {jsonObjectSchema} from './json.ts';

/**
 * Indicates that replication cannot continue and that the replica must be resynced
 * from scratch. The replication-manager will shutdown in response to this message,
 * and upon being restarted, it will wipe the current replica and resync if the
 * `--auto-reset` option is specified.
 *
 * This signal should only be used in well advertised scenarios, and is not suitable
 * as a common occurrence in production.
 */
export const resetRequiredSchema = v.object({
  tag: v.literal('reset-required'),
  message: v.string().optional(),

  // errorDetails can contain any standard JSON object. This will be
  // published in the `errorDetails` field of a replication ERROR event.
  errorDetails: jsonObjectSchema.optional(),
});
