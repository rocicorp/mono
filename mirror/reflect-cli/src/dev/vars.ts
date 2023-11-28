import {compareUTF8} from 'compare-utf8';
import {existsSync, mkdirSync, readFileSync, writeFileSync} from 'fs';
import {
  ALLOWED_SERVER_VARIABLE_CHARS,
  MAX_SERVER_VARIABLES,
  variableIsWithinSizeLimit,
  variableNameIsWithinSizeLimit,
} from 'mirror-schema/src/external/vars.js';
import path from 'path';
import {getProperties} from 'properties-file';
import {escapeKey, escapeValue} from 'properties-file/escape';
import {mustFindAppConfigRoot} from '../app-config.js';
import {UserError} from '../error.js';

export function listDevVars(): Record<string, string> {
  const {file: varsFile} = varsFilePaths();
  if (existsSync(varsFile)) {
    return validateAndSort(getProperties(readFileSync(varsFile, 'utf-8')));
  }
  return {};
}

export function setDevVars(newVars: Record<string, string>) {
  const devVars = listDevVars();
  Object.entries(newVars).forEach(([key, value]) => {
    devVars[key] = value;
  });
  saveDevVars(devVars);
}

export function deleteDevVars(vars: string[]) {
  const devVars = listDevVars();
  vars.forEach(key => {
    delete devVars[key];
  });
  saveDevVars(devVars);
}

let fileOverrideForTests: {dir: string; file: string} | undefined;

export function setFileOverrideForTests(
  override: {dir: string; file: string} | undefined,
) {
  fileOverrideForTests = override;
}

function varsFilePaths(): {dir: string; file: string} {
  if (fileOverrideForTests) {
    return fileOverrideForTests;
  }
  const appConfigRoot = mustFindAppConfigRoot();
  const dir = path.join(appConfigRoot, '.reflect');
  return {dir, file: path.join(dir, 'dev-vars.env')};
}

function validateAndSort(devVars: Record<string, string>) {
  const entries = Object.entries(devVars);
  if (entries.length > MAX_SERVER_VARIABLES) {
    throw new UserError(
      `Up to ${MAX_SERVER_VARIABLES} variables are allowed.\n` +
        `Use 'npx @rocicrop vars delete --dev' to remove unused variables.`,
    );
  }
  entries.forEach(([key, value]) => {
    if (!ALLOWED_SERVER_VARIABLE_CHARS.test(key)) {
      throw new UserError(
        `Invalid key "${key}". Variables may only contain alphanumeric characters and underscores.`,
      );
    }
    if (!variableNameIsWithinSizeLimit(key)) {
      throw new UserError(
        `Variable name "${key}" exceeds the maximum size limit. UTF-8 encoded names must not exceed 1 kilobyte.`,
      );
    }
    if (!variableIsWithinSizeLimit(key, value)) {
      throw new UserError(
        `Variable "${key}" exceeds the maximum size limit. UTF-8 encoded variables (key+value) must not exceed 5 kilobytes.`,
      );
    }
  });
  const sorted = entries.sort(([a], [b]) => compareUTF8(a, b));
  return Object.fromEntries(sorted);
}

function saveDevVars(devVars: Record<string, string>) {
  const sorted = validateAndSort(devVars);
  const contents = Object.entries(sorted)
    .map(([key, value]) => `${escapeKey(key)}=${escapeValue(value)}`)
    .join('\n');

  const {dir, file: varsFile} = varsFilePaths();
  mkdirSync(dir, {recursive: true});
  writeFileSync(varsFile, contents, 'utf-8');
}
