import type {IncomingHttpHeaders} from 'node:http';
import {must} from '../../../shared/src/must.ts';
import {ANALYZE_FILTER_NODE_PROTOCOL_VERSION} from '../../../zero-protocol/src/analyze-query-result.ts';
import {
  decodeSecProtocols,
  getFeatureFlags,
  type FeatureFlags,
  type InitConnectionMessage,
} from '../../../zero-protocol/src/connect.ts';
import {FeatureFlag} from '../../../zero-protocol/src/feature-flag.ts';
import {POKE_CHUNK_PROTOCOL_VERSION} from '../../../zero-protocol/src/poke.ts';
import {URLParams} from '../types/url-params.ts';

export type ConnectParams = {
  readonly protocolVersion: number;
  readonly clientID: string;
  readonly clientGroupID: string;
  readonly profileID: string | null;
  readonly baseCookie: string | null;
  readonly timestamp: number;
  readonly lmID: number;
  readonly wsID: string;
  readonly debugPerf: boolean;
  readonly features: FeatureFlagSet;
  readonly auth: string | undefined;
  readonly userID: string | undefined;
  readonly initConnectionMsg: InitConnectionMessage | undefined;
  readonly httpCookie: string | undefined;
  readonly origin: string | undefined;
  readonly requestHeaders?: Readonly<Record<string, string>> | undefined;
  readonly generation?: number | undefined;
};

/**
 * Normalizes Node's {@link IncomingHttpHeaders} (whose values are
 * `string | string[] | undefined`) into a plain `Record<string, string>`,
 * joining array values with `, ` and dropping `undefined` values.
 */
function normalizeHeaders(
  headers: IncomingHttpHeaders,
): Record<string, string> {
  const normalized: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined) {
      continue;
    }
    normalized[key] = Array.isArray(value) ? value.join(', ') : value;
  }
  return normalized;
}

/**
 * The protocol version from which a client that doesn't send a flag gets the
 * feature. Clients below it get the feature only by sending the flag.
 */
const featureOnByDefaultFrom = {
  [FeatureFlag.PokeChunk]: POKE_CHUNK_PROTOCOL_VERSION,
  [FeatureFlag.AnalyzeFilterNode]: ANALYZE_FILTER_NODE_PROTOCOL_VERSION,
} as const satisfies Record<FeatureFlag, number>;

/** The features a client gets. See {@link resolveFeatures}. */
export type FeatureFlagSet = ReadonlySet<FeatureFlag>;

/** `Set`, typed so that `new FeatureFlagSet()` needs no type argument. */
export const FeatureFlagSet = Set<FeatureFlag>;

/**
 * The features a client gets: the flags it sent, and for the flags it didn't
 * send, the features its protocol version has on by default.
 */
export function resolveFeatures(
  protocolVersion: number,
  featureFlags: FeatureFlags,
): FeatureFlagSet {
  const features = new FeatureFlagSet();
  for (const flag of Object.values(FeatureFlag)) {
    if (
      featureFlags.get(flag) ??
      protocolVersion >= featureOnByDefaultFrom[flag]
    ) {
      features.add(flag);
    }
  }
  return features;
}

export function getConnectParams(
  protocolVersion: number,
  url: URL,
  headers: IncomingHttpHeaders,
):
  | {
      params: ConnectParams;
      error: null;
    }
  | {
      params: null;
      error: string;
    } {
  const params = new URLParams(url);

  try {
    const clientID = params.get('clientID', true);
    const clientGroupID = params.get('clientGroupID', true);
    const profileID = params.get('profileID', false);
    const baseCookie = params.get('baseCookie', false);
    const timestamp = params.getInteger('ts', true);
    const lmID = params.getInteger('lmid', true);
    const wsID = params.get('wsid', false) ?? '';
    const userID = params.get('userID', false) ?? undefined;
    const debugPerf = params.getBoolean('debugPerf');
    const features = resolveFeatures(
      protocolVersion,
      getFeatureFlags(url.searchParams),
    );
    const {initConnectionMessage, authToken} = decodeSecProtocols(
      must(headers['sec-websocket-protocol']),
    );

    return {
      params: {
        protocolVersion,
        clientID,
        clientGroupID,
        profileID,
        baseCookie,
        timestamp,
        lmID,
        wsID,
        debugPerf,
        features,
        initConnectionMsg: initConnectionMessage,
        auth: authToken,
        userID,
        httpCookie: headers.cookie,
        origin: headers.origin,
        requestHeaders: normalizeHeaders(headers),
      },
      error: null,
    };
  } catch (e) {
    return {
      params: null,
      error: e instanceof Error ? e.message : String(e),
    };
  }
}
