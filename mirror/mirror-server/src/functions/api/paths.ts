import {HttpsError} from 'firebase-functions/v2/https';
import {match} from 'path-to-regexp';
import {unsupportedMethodError} from './errors.js';

export type ReadParams = {
  appID: string;
  resource: string;
  subpath?: string;
};

export type WriteParams = {
  appID: string;
  resource: string;
  subpath?: string;
  command: string;
};

export type PathParams = {
  appID: string;
  resource: string;
  subpath?: string;
  command?: string;
};

const matchReadParams = match<ReadParams>(
  '/v1/apps/:appID/:resource{/:subpath([^:]*)}?',
);

const matchWriteParams = match<WriteParams>(
  '/v1/apps/:appID/:resource{/:subpath([^:]*)}?\\::command',
);

export function parseReadParams(path: string): ReadParams {
  const matched = matchReadParams(path);
  if (!matched) {
    if (matchWriteParams(path)) {
      throw unsupportedMethodError();
    }
    throw new HttpsError('not-found', 'Unknown or malformed url');
  }
  return matched.params;
}

export function parseWriteParams(path: string): WriteParams {
  const matched = matchWriteParams(path);
  if (!matched) {
    if (matchReadParams(path)) {
      throw unsupportedMethodError();
    }
    throw new HttpsError('not-found', 'Unknown or malformed url');
  }
  return matched.params;
}

const WORKER_PATH_PREFIX = '/api/v1/';

export function makeWorkerPath(params: PathParams): string {
  const parts = [WORKER_PATH_PREFIX, params.resource];
  if (params.subpath) {
    parts.push('/');
    parts.push(params.subpath);
  }
  if (params.command) {
    parts.push(':');
    parts.push(params.command);
  }
  return parts.join('');
}
