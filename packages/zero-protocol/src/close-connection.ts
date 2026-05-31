import * as v from '../../shared/src/valita.ts';

/**
 * We do not use the body yet.
 */
export const closeConnectionBodySchema = v.array(v.unknown());

/**
 * This message gets sent as part of the close reason in the WebSocket close event.
 * The close reason is a string, so we serialize this message to JSON.
 
 * "The value must be no longer than 123 bytes (encoded in UTF-8)." -
 * https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/close#reason
 * 
 * @deprecated
 */
export const closeConnectionMessageSchema = v.tuple([
  v.literal('closeConnection'),
  closeConnectionBodySchema,
]);

/** @deprecated */
export type CloseConnectionBody = v.Infer<typeof closeConnectionBodySchema>;

/** @deprecated */
export type CloseConnectionMessage = v.Infer<
  typeof closeConnectionMessageSchema
>;
