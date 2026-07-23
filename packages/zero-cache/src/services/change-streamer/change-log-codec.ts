import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {ChangeTag, WatermarkedChange} from './change-streamer.ts';

export type ChangeLogEntry = {
  watermark: string;
  tag: string;
  change: string;
};

/** Serializes a data-plane message to the canonical downstream JSON form. */
export function serializeChangeStreamData(data: ChangeStreamData): string {
  return BigIntJSON.stringify(data);
}

/**
 * Extracts the stringified change message from the stringified stream message
 * (the second tuple element). This allows the stream message to be stringified
 * once while storing only the change message in the change log for backwards
 * compatibility.
 */
export function extractChangeSubstring(
  streamMessageJSON: string,
  tag: ChangeTag | undefined,
): string {
  switch (tag) {
    case 'begin':
    case 'commit':
      // e.g.
      // ["begin",<message-json>,{"commitWatermark":"92fj2d0s"}]
      // ["commit",<message-json>,{"watermark":"92fj2d0s"}]
      return streamMessageJSON.substring(
        streamMessageJSON.indexOf(',') + 1,
        streamMessageJSON.lastIndexOf(','),
      );
    default:
      // ["data",<message-json>]
      return streamMessageJSON.substring(
        streamMessageJSON.indexOf(',') + 1,
        streamMessageJSON.lastIndexOf(']'),
      );
  }
}

/** Reconstructs the canonical downstream JSON around a stored change. */
export function reconstructWatermarkedChange(
  entry: ChangeLogEntry,
): WatermarkedChange {
  const {watermark, change} = entry;
  const tag = entry.tag as ChangeTag;
  switch (tag) {
    case 'begin':
      return [
        watermark,
        tag,
        `["begin",${change},{"commitWatermark":"${watermark}"}]`,
      ];
    case 'commit':
      return [
        watermark,
        tag,
        `["commit",${change},{"watermark":"${watermark}"}]`,
      ];
    case 'rollback':
      return [watermark, tag, `["rollback",${change}]`];
    default:
      return [watermark, tag, `["data",${change}]`];
  }
}
