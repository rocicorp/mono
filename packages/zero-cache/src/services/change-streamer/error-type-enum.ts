export const Unknown = 0;
export const WrongReplicaVersion = 1;
export const WatermarkTooOld = 2;
/**
 * Sent on an initially live stream before disconnecting it because
 * it is too far behind. How the subscriber handles this is unspecified
 * by the server; the current implementation shuts down the process
 * with the assumption that it is in a pathological zombie state.
 */
export const StreamTooFarBehind = 3;

export type Unknown = typeof Unknown;
export type WrongReplicaVersion = typeof WrongReplicaVersion;
export type WatermarkTooOld = typeof WatermarkTooOld;
export type StreamTooFarBehind = typeof StreamTooFarBehind;
