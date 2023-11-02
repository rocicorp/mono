import {compareUTF8} from 'compare-utf8';
import {parse} from 'dotenv';
import {existsSync, readFileSync, writeFileSync} from 'fs';
import {
  ALLOWED_SERVER_VARIABLE_CHARS,
  MAX_SERVER_VARIABLES,
} from 'mirror-schema/src/external/vars.js';
import path from 'path';
import {mustFindAppConfigRoot} from '../app-config.js';
import {UserError} from '../error.js';

export function listDevVars(): Record<string, string> {
  const varsFile = varsFilePath();
  if (existsSync(varsFile)) {
    return validateAndSort(parse(readFileSync(varsFile, 'utf-8')));
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

let fileOverrideForTests: string | undefined;

export function setFileOverriddeForTests(path: string | undefined) {
  fileOverrideForTests = path;
}

function varsFilePath() {
  if (fileOverrideForTests) {
    return fileOverrideForTests;
  }
  const appConfigRoot = mustFindAppConfigRoot();
  return path.join(appConfigRoot, '.reflect', 'dev-vars.env');
}

function validateAndSort(devVars: Record<string, string>) {
  const keys = Object.keys(devVars);
  if (keys.length > MAX_SERVER_VARIABLES) {
    throw new UserError(
      `Up to ${MAX_SERVER_VARIABLES} variables are allowed.\n` +
        `Use 'npx @rocicrop vars delete --dev' to remove unused variables.`,
    );
  }
  keys.forEach(key => {
    if (!ALLOWED_SERVER_VARIABLE_CHARS.test(key)) {
      throw new UserError(
        `Invalid key "${key}". Variables may only contain alphanumeric characters and underscores.`,
      );
    }
  });
  const sorted = Object.entries(devVars).sort(([a], [b]) => compareUTF8(a, b));
  return Object.fromEntries(sorted);
}

function saveDevVars(devVars: Record<string, string>) {
  const sorted = validateAndSort(devVars);
  const contents = Object.entries(sorted)
    .map(([key, value]) => `${key}=${value}`)
    .join('\n');

  const varsFile = varsFilePath();
  writeFileSync(varsFile, contents, 'utf-8');
}
