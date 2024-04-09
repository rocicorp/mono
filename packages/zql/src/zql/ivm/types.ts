export type Version = number;
export type StringOrNumber = string | number;

export const joinSymbol = Symbol();

type JoinResultBase = {
  id: StringOrNumber;
  [joinSymbol]: true;
};

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
  return (
    typeof x === 'object' && x !== null && (x as JoinResultBase)[joinSymbol]
  );
}
