/* eslint-disable @typescript-eslint/naming-convention */

export const Unknown = 0;
export const WrongReplicaVersion = 1;
export const WatermarkTooOld = 2;
export const WatermarkNotFound = 3;

export type Unknown = typeof Unknown;
export type WrongReplicaVersion = typeof WrongReplicaVersion;
export type WatermarkTooOld = typeof WatermarkTooOld;
export type WatermarkNotFound = typeof WatermarkNotFound;
