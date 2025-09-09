import type {LogContext} from '@rocicorp/logger';
import * as v from '../../../shared/src/valita.ts';
import type {StatementRunner} from '../db/statements.ts';
import {elide} from '../types/strings.ts';
import {indicesConfigSchema, type IndicesConfig} from './indices-config.ts';

export type LoadedIndices = {
  indices: IndicesConfig | undefined;
  hash: string | undefined;
};

export function loadIndices(
  lc: LogContext,
  replica: StatementRunner,
  appID: string,
): LoadedIndices {
  const {indices, hash} = replica.get(
    `SELECT indices, hash FROM "${appID}.indices"`,
  );
  if (indices == null) {
    lc.info?.(
      `No upstream indices deployed for ${appID}. ` +
        `Fulltext search will not be available.`,
    );
    return {indices, hash: undefined};
  }
  let obj;
  let parsed;
  try {
    obj = JSON.parse(indices);
    parsed = v.parse(obj, indicesConfigSchema);
  } catch (e) {
    throw new Error(
      `Could not parse upstream indices: ` +
        `'${elide(String(indices), 100)}'.\n` +
        `This may happen if indices with a new internal format are ` +
        `deployed before the supporting server has been fully rolled out.`,
      {cause: e},
    );
  }
  return {indices: parsed, hash};
}

export function reloadIndicesIfChanged(
  lc: LogContext,
  replica: StatementRunner,
  appID: string,
  current: LoadedIndices | null,
): {indices: LoadedIndices; changed: boolean} {
  if (current === null) {
    return {indices: loadIndices(lc, replica, appID), changed: true};
  }
  const {hash} = replica.get(`SELECT hash FROM "${appID}.indices"`);
  return hash === current.hash
    ? {indices: current, changed: false}
    : {indices: loadIndices(lc, replica, appID), changed: true};
}
