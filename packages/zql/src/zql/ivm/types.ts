import {assert} from 'shared/src/asserts.js';

export type Version = number;
export type StringOrNumber = string | number;

export const joinSymbol = Symbol();

type JoinResultBase = {
  id: StringOrNumber;
  [joinSymbol]: true;
};

export type PipelineEntity = Record<string, unknown>;

export type Comparator<T> = (l: T, r: T) => number;

export type JoinResult<
  AValue,
  BValue,
  AAlias extends string | unknown,
  BAlias extends string | unknown,
> = JoinResultBase &
  (AValue extends JoinResultBase
    ? AValue
    : {
        [K in AAlias extends string ? AAlias : never]: AValue;
      }) &
  (BValue extends JoinResultBase
    ? BValue
    : {[K in BAlias extends string ? BAlias : never]: BValue});

export function isJoinResult(x: unknown): x is JoinResultBase {
  // eslint-disable-next-line eqeqeq
  return x != null && (x as JoinResultBase)[joinSymbol];
}

export function assertStringOrNumber(v: unknown): asserts v is StringOrNumber {
  assert(typeof v === 'string' || typeof v === 'number');
}
