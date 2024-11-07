import {assert} from '../../../shared/src/asserts.js';

export type HTTPString = `http${'' | 's'}://${string}`;

export type WSString = `ws${'' | 's'}://${string}`;

export function toWSString(url: HTTPString): WSString {
  return ('ws' + url.slice(4)) as WSString;
}

export function toHTTPString(url: WSString): HTTPString {
  return ('http' + url.slice(2)) as HTTPString;
}

export function assertHTTPString(url: string): asserts url is HTTPString {
  assert(/^https?:\/\//.test(url));
}

export function assertWSString(url: string): asserts url is WSString {
  assert(/^wss?:\/\//.test(url));
}

export function appendPath<T extends HTTPString | WSString>(
  url: T,
  toAppend: `/${string}`,
): T {
  return (url + (url.endsWith('/') ? toAppend.substring(1) : toAppend)) as T;
}
