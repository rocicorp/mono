/* eslint-disable @typescript-eslint/naming-convention */

export const ChunkData = 0;
export const ChunkMeta = 1;
export const ChunkRefCount = 2;
export const Head = 3;

export type ChunkData = typeof ChunkData;
export type ChunkMeta = typeof ChunkMeta;
export type ChunkRefCount = typeof ChunkRefCount;
export type Head = typeof Head;

export type Type = ChunkData | ChunkMeta | ChunkRefCount | Head;
