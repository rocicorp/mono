import {type Hash, parse as parseHash} from '../hash.js';

export function chunkDataKey(hash: Hash): string {
  return `c/${hash}/d`;
}

export function chunkMetaKey(hash: Hash): string {
  return `c/${hash}/m`;
}

export function chunkRefCountKey(hash: Hash): string {
  return `c/${hash}/r`;
}

export function headKey(name: string): string {
  return `h/${name}`;
}

export const enum KeyType {
  ChunkData,
  ChunkMeta,
  ChunkRefCount,
  Head,
}

export type Key =
  | {
      type: KeyType.ChunkData;
      hash: Hash;
    }
  | {
      type: KeyType.ChunkMeta;
      hash: Hash;
    }
  | {
      type: KeyType.ChunkRefCount;
      hash: Hash;
    }
  | {
      type: KeyType.Head;
      name: string;
    };

export function parse(key: string): Key {
  const invalidKey = () => new Error(`Invalid key. Got "${key}"`);
  const hash = () => parseHash(key.substring(2, key.length - 2));

  // '/'
  if (key.charCodeAt(1) === 47) {
    switch (key.charCodeAt(0)) {
      // c
      case 99: {
        if (key.length < 4 || key.charCodeAt(key.length - 2) !== 47) {
          throw invalidKey();
        }
        switch (key.charCodeAt(key.length - 1)) {
          case 100: // d
            return {
              type: KeyType.ChunkData,
              hash: hash(),
            };
          case 109: // m
            return {
              type: KeyType.ChunkMeta,
              hash: hash(),
            };
          case 114: // r
            return {
              type: KeyType.ChunkRefCount,
              hash: hash(),
            };
        }
        break;
      }
      case 104: // h
        return {
          type: KeyType.Head,
          name: key.substring(2),
        };
    }
  }
  throw invalidKey();
}
