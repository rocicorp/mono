/**
 * Wire-format representation of the zql AST interface.
 *
 * `v.Type<...>` types are explicitly declared to facilitate Typescript verification
 * that the schemas satisfy the zql type definitions. (Incidentally, explicit types
 * are also required for recursive schema definitions.)
 */

import type {Condition, Join} from '@rocicorp/zql/src/zql/ast/ast.js';
import * as v from 'shared/src/valita.js';

function readonly<T>(t: v.Type<T>): v.Type<Readonly<T>> {
  return t as v.Type<Readonly<T>>;
}

export const selectorSchema = readonly(v.tuple([v.string(), v.string()]));

const orderingElementSchema = readonly(
  v.tuple([selectorSchema, v.union(v.literal('asc'), v.literal('desc'))]),
);

export const orderingSchema = readonly(v.array(orderingElementSchema));

export const primitiveSchema = v.union(
  v.string(),
  v.number(),
  v.boolean(),
  v.null(),
);

export const primitiveArraySchema = v.union(
  v.array(v.string()),
  v.array(v.number()),
  v.array(v.boolean()),
);

export const aggregateSchema = v.union(
  v.literal('sum'),
  v.literal('avg'),
  v.literal('min'),
  v.literal('max'),
  v.literal('array'),
  v.literal('count'),
);

export const aggregationSchema = v.object({
  field: selectorSchema.optional(),
  alias: v.string(),
  aggregate: aggregateSchema,
});

// Split out so that its inferred type can be checked against
// Omit<Join, 'other'> in ast-type-test.ts.
// The mutually-recursive reference of the 'other' field to astSchema
// is the only thing added in v.lazy.  The v.lazy is necessary due to the
// mutually-recursive types, but v.lazy prevents inference of the resulting
// type.
export const joinOmitOther = v.object({
  type: v.union(
    v.literal('inner'),
    v.literal('left'),
    v.literal('right'),
    v.literal('full'),
  ),
  as: v.string(),
  on: v.tuple([selectorSchema, selectorSchema]),
});

export const joinSchema: v.Type<Join> = v.lazy(() =>
  joinOmitOther.extend({
    other: astSchema,
  }),
);

export const conditionSchema: v.Type<Condition> = v.lazy(() =>
  v.union(simpleConditionSchema, conjunctionSchema),
);

export const conjunctionSchema = v.object({
  type: v.literal('conjunction'),
  op: v.union(v.literal('AND'), v.literal('OR')),
  conditions: v.array(conditionSchema),
});

export const astSchema = v.object({
  schema: v.string().optional(),
  table: v.string(),
  alias: v.string().optional(),
  select: readonly(
    v.array(readonly(v.tuple([selectorSchema, v.string()]))),
  ).optional(),
  aggregate: v.array(aggregationSchema).optional(),
  where: conditionSchema.optional(),
  joins: v.array(joinSchema).optional(),
  limit: v.number().optional(),
  groupBy: v.array(selectorSchema).optional(),
  orderBy: orderingSchema.optional(),
});

export const equalityOpsSchema = v.union(v.literal('='), v.literal('!='));

export const orderOpsSchema = v.union(
  v.literal('<'),
  v.literal('>'),
  v.literal('<='),
  v.literal('>='),
);

export const inOpsSchema = v.union(v.literal('IN'), v.literal('NOT IN'));

export const likeOpsSchema = v.union(
  v.literal('LIKE'),
  v.literal('NOT LIKE'),
  v.literal('ILIKE'),
  v.literal('NOT ILIKE'),
);

export const setOpsSchema = v.union(
  v.literal('INTERSECTS'),
  v.literal('DISJOINT'),
  v.literal('SUPERSET'),
  v.literal('CONGRUENT'),
  v.literal('INCONGRUENT'),
  v.literal('SUBSET'),
);

export const simpleOperatorSchema = v.union(
  equalityOpsSchema,
  orderOpsSchema,
  inOpsSchema,
  likeOpsSchema,
  setOpsSchema,
);

export const simpleConditionSchema = v.object({
  type: v.literal('simple'),
  op: simpleOperatorSchema,
  field: selectorSchema,
  value: v.object({
    type: v.literal('value'),
    value: v.union(primitiveSchema, primitiveArraySchema),
  }),
});
