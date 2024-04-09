import {unreachable} from '../error/asserts.js';

export function compareEntityFields<T>(lVal: T, rVal: T) {
  if (lVal === rVal) {
    return 0;
  }
  if (lVal === null || lVal === undefined) {
    return -1;
  }
  if (rVal === null || rVal === undefined) {
    return 1;
  }
  if (lVal < rVal) {
    return -1;
  }
  if (lVal > rVal) {
    return 1;
  }

  if (lVal instanceof Date && rVal instanceof Date) {
    return lVal.getTime() - rVal.getTime();
  }

  unreachable();
}
