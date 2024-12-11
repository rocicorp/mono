import type {IncomingMessage} from 'http';

/**
 * Contains the subset of {@link IncomingMessage} fields suitable for
 * passing across processes.
 */
export type IncomingMessageSubset = Pick<
  IncomingMessage,
  | 'headers'
  | 'headersDistinct'
  | 'httpVersion'
  | 'method'
  | 'rawHeaders'
  | 'rawTrailers'
  | 'trailers'
  | 'trailersDistinct'
  | 'url'
>;

export function serializableSubset(
  msg: IncomingMessageSubset,
): IncomingMessageSubset {
  const {
    headers,
    headersDistinct,
    httpVersion,
    method = 'GET',
    rawHeaders,
    rawTrailers,
    trailers,
    trailersDistinct,
    url,
  } = msg;

  return {
    headers,
    headersDistinct,
    httpVersion,
    method,
    rawHeaders,
    rawTrailers,
    trailers,
    trailersDistinct,
    url,
  };
}
