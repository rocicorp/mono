import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {ChangeTag, WatermarkedChange} from './change-streamer.ts';

export type ChangeLogEntry = {
  watermark: string;
  tag: string;
  change: string;
};

export type SerializedChangeStreamData = {
  /** The canonical downstream representation forwarded to subscribers. */
  readonly json: string;
  /** The already-serialized change tuple element stored in change logs. */
  readonly change: string;
};

/**
 * Serializes a data-plane message and its stored change payload in one pass.
 *
 * Change logs retain only the second tuple element for backwards
 * compatibility. Serializing that element first avoids stringifying the full
 * message and then copying the payload back out of the resulting string on the
 * replication hot path.
 */
export function serializeChangeStreamDataWithChange(
  data: ChangeStreamData,
): SerializedChangeStreamData {
  const change = BigIntJSON.stringify(data[1]);
  const type = data[0];
  switch (type) {
    case 'begin':
    case 'commit':
      return {
        json: `[${JSON.stringify(type)},${change},${BigIntJSON.stringify(data[2])}]`,
        change,
      };
    case 'data':
    case 'rollback':
      return {json: `[${JSON.stringify(type)},${change}]`, change};
  }
}

/** Serializes a data-plane message to the canonical downstream JSON form. */
export function serializeChangeStreamData(data: ChangeStreamData): string {
  return serializeChangeStreamDataWithChange(data).json;
}

/**
 * Extracts the second tuple element for callers that only have the canonical
 * downstream JSON. The live write path uses
 * {@link serializeChangeStreamDataWithChange} and does not scan or copy the
 * completed message.
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
