import type {DefaultParams} from 'wouter';
import {must} from '../../../../../packages/shared/src/must.ts';

export function getID(params: DefaultParams) {
  const idStr = must(params.id);
  return getIDFromString(idStr);
}

export function getIDFromString(idStr: string) {
  const idField = /[^\d]/.test(idStr) ? 'id' : 'shortID';
  const id = idField === 'shortID' ? parseInt(idStr) : idStr;
  return {idField, id} as const;
}
