import {must} from 'shared/src/must.ts';
import type {DefaultParams} from 'wouter';

export function getID(params: DefaultParams) {
  const idStr = must(params.id);
  return getIDFromString(idStr);
}

const nonDigitRegex = /[^\d]/;

export function getIDFromString(idStr: string) {
  const idField = nonDigitRegex.test(idStr) ? 'id' : 'shortID';
  const idValue = idField === 'shortID' ? parseInt(idStr) : idStr;
  return {idField, idValue} as const;
}
