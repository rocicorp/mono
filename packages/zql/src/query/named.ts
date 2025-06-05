import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {SchemaQuery} from '../mutate/custom.ts';
import type {Query} from './query.ts';

export type NamedQuery<
  S extends Schema,
  TArg extends ReadonlyArray<ReadonlyJSONValue> = ReadonlyJSONValue[],
> = (
  tx: SchemaQuery<S>,
  ...args: TArg
) => {
  name: string;
  args: TArg;
  query: Query<S, keyof S['tables'] & string>;
};

type NamedQueryFunc<
  S extends Schema,
  TArg extends ReadonlyArray<ReadonlyJSONValue> = ReadonlyJSONValue[],
> = (tx: SchemaQuery<S>, ...arg: TArg) => Query<S, keyof S['tables'] & string>;

export function query<
  S extends Schema,
  TArg extends ReadonlyArray<ReadonlyJSONValue> = ReadonlyJSONValue[],
>(_s: S, name: string, fn: NamedQueryFunc<S, TArg>): NamedQuery<S, TArg> {
  return function queryWrapper(tx: SchemaQuery<S>, ...args: TArg) {
    return {
      name,
      args,
      query: fn(tx, ...args),
    };
  };
}

query.bindTo =
  <S extends Schema>(s: S) =>
  <TArg extends ReadonlyArray<ReadonlyJSONValue> = ReadonlyJSONValue[]>(
    name: string,
    fn: NamedQueryFunc<S, TArg>,
  ): NamedQuery<S, TArg> =>
    query(s, name, fn);
