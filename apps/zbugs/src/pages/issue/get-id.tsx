import type {DefaultParams} from 'wouter';
import {must} from '../../../../../packages/shared/src/must.ts';
import {getIDFromString} from '../../../shared/issue-id.ts';

export {getIDFromString};

export function getID(params: DefaultParams) {
  const idStr = must(params.id);
  return getIDFromString(idStr);
}
