/* eslint-disable @typescript-eslint/naming-convention */

export const SDD = 4;
export const DD31 = 5;
// V6 added refreshHashes and persistHash to Client to fix ChunkNotFound errors
export const V6 = 6;
// V7 added sizeOfEntry to the BTree chunk data.
export const V7 = 7;
export const Latest = V7;

export type SDD = typeof SDD;
export type DD31 = typeof DD31;
export type V6 = typeof V6;
export type V7 = typeof V7;
export type Latest = typeof Latest;

export type Type = SDD | DD31 | V6 | V7 | Latest;
export type {Type as FormatVersion};
