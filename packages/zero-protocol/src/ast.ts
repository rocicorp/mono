/**
 * Wire-format representation of the zql AST interface.
 *
 * `v.Type<...>` types are explicitly declared to facilitate Typescript verification
 * that the schemas satisfy the zql type definitions. (Incidentally, explicit types
 * are also required for recursive schema definitions.)
 */

import type {
  AST,
  Aggregate,
  Aggregation,
  Condition,
  Conjunction,
  EqualityOps,
  InOps,
  Join,
  LikeOps,
  OrderOps,
  Ordering,
  Primitive,
  PrimitiveArray,
  Selector,
  SetOps,
  SimpleCondition,
  SimpleOperator,
} from '@rocicorp/zql/src/zql/ast/ast.js';
import * as v from 'shared/src/valita.js';

function readonly<T>(t: v.Type<T>): v.Type<Readonly<T>> {
  return t as v.Type<Readonly<T>>;
}

export const selectorSchema = readonly(v.tuple([v.string(), v.string()]));

// The following ensures Selector and selectorSchema
// are kept in sync (each type satisfies the other).
(t: Selector, inferredT: v.Infer<typeof selectorSchema>) => {
  t satisfies v.Infer<typeof selectorSchema>;
  inferredT satisfies Selector;
};

export const orderingSchema = readonly(
  v.tuple([
    readonly(v.array(selectorSchema)),
    v.union(v.literal('asc'), v.literal('desc')),
  ]),
);

// The following ensures Ordering and orderingSchema
// are kept in sync (each type satisfies the other).
(t: Ordering, inferredT: v.Infer<typeof orderingSchema>) => {
  t satisfies v.Infer<typeof orderingSchema>;
  inferredT satisfies Ordering;
};

export const primitiveSchema = v.union(
  v.string(),
  v.number(),
  v.boolean(),
  v.null(),
);

// The following ensures Primitive and primitiveSchema
// are kept in sync (each type satisfies the other).
(t: Primitive, inferredT: v.Infer<typeof primitiveSchema>) => {
  t satisfies v.Infer<typeof primitiveSchema>;
  inferredT satisfies Primitive;
};

export const primitiveArraySchema = v.union(
  v.array(v.string()),
  v.array(v.number()),
  v.array(v.boolean()),
);

// The following ensures PrimitiveArray and primitiveArraySchema
// are kept in sync (each type satisfies the other).
(t: PrimitiveArray, inferredT: v.Infer<typeof primitiveArraySchema>) => {
  t satisfies v.Infer<typeof primitiveArraySchema>;
  inferredT satisfies PrimitiveArray;
};

export const aggregateSchema = v.union(
  v.literal('sum'),
  v.literal('avg'),
  v.literal('min'),
  v.literal('max'),
  v.literal('array'),
  v.literal('count'),
);

// The following ensures Aggregate and aggregateSchema
// are kept in sync (each type satisfies the other).
(t: Aggregate, inferredT: v.Infer<typeof aggregateSchema>) => {
  t satisfies v.Infer<typeof aggregateSchema>;
  inferredT satisfies Aggregate;
};

export const aggregationSchema = v.object({
  field: selectorSchema.optional(),
  alias: v.string(),
  aggregate: aggregateSchema,
});

// The following ensures Aggregation and aggregationSchema
// are kept in sync (each type satisfies the other).
(t: Aggregation, inferredT: v.Infer<typeof aggregationSchema>) => {
  t satisfies v.Infer<typeof aggregationSchema>;
  inferredT satisfies Aggregation;
};

export const joinSchema: v.Type<Join> = v.lazy(() =>
  v.object({
    type: v.union(
      v.literal('inner'),
      v.literal('left'),
      v.literal('right'),
      v.literal('full'),
    ),
    other: astSchema,
    as: v.string(),
    on: v.tuple([selectorSchema, selectorSchema]),
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

// The following ensures Conjunction and conjunctionSchema
// are kept in sync (each type satisfies the other).
(t: Conjunction, inferredT: v.Infer<typeof conjunctionSchema>) => {
  t satisfies v.Infer<typeof conjunctionSchema>;
  inferredT satisfies Conjunction;
};

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

// The following ensures AST and astSchema
// are kept in sync (each type satisfies the other).
(t: AST, inferredT: v.Infer<typeof astSchema>) => {
  t satisfies v.Infer<typeof astSchema>;
  inferredT satisfies AST;
};

export const equalityOpsSchema = v.union(v.literal('='), v.literal('!='));

// The following ensures EqualityOps and equalityOpsSchema
// are kept in sync (each type satisfies the other).
(t: EqualityOps, inferredT: v.Infer<typeof equalityOpsSchema>) => {
  t satisfies v.Infer<typeof equalityOpsSchema>;
  inferredT satisfies EqualityOps;
};

export const orderOpsSchema = v.union(
  v.literal('<'),
  v.literal('>'),
  v.literal('<='),
  v.literal('>='),
);

// The following ensures OrderOps and orderOpsSchema
// are kept in sync (each type satisfies the other).
(t: OrderOps, inferredT: v.Infer<typeof orderOpsSchema>) => {
  t satisfies v.Infer<typeof orderOpsSchema>;
  inferredT satisfies OrderOps;
};

export const inOpsSchema = v.union(v.literal('IN'), v.literal('NOT IN'));

// The following ensures OrderOps and inOpsSchema
// are kept in sync (each type satisfies the other).
(t: InOps, inferredT: v.Infer<typeof inOpsSchema>) => {
  t satisfies v.Infer<typeof inOpsSchema>;
  inferredT satisfies InOps;
};

export const likeOpsSchema = v.union(
  v.literal('LIKE'),
  v.literal('NOT LIKE'),
  v.literal('ILIKE'),
  v.literal('NOT ILIKE'),
);

// The following ensures LikeOps and likeOpsSchema
// are kept in sync (each type satisfies the other).
(t: LikeOps, inferredT: v.Infer<typeof likeOpsSchema>) => {
  t satisfies v.Infer<typeof likeOpsSchema>;
  inferredT satisfies LikeOps;
};

export const setOpsSchema = v.union(
  v.literal('INTERSECTS'),
  v.literal('DISJOINT'),
  v.literal('SUPERSET'),
  v.literal('CONGRUENT'),
  v.literal('INCONGRUENT'),
  v.literal('SUBSET'),
);

// The following ensures SetOps and setOpsSchema
// are kept in sync (each type satisfies the other).
(t: SetOps, inferredT: v.Infer<typeof setOpsSchema>) => {
  t satisfies v.Infer<typeof setOpsSchema>;
  inferredT satisfies SetOps;
};

export const simpleOperatorSchema = v.union(
  equalityOpsSchema,
  orderOpsSchema,
  inOpsSchema,
  likeOpsSchema,
  setOpsSchema,
);

// The following ensures SimpleOperator and simpleOperatorSchema
// are kept in sync (each type satisfies the other).
(t: SimpleOperator, inferredT: v.Infer<typeof simpleOperatorSchema>) => {
  t satisfies v.Infer<typeof simpleOperatorSchema>;
  inferredT satisfies SimpleOperator;
};

export const simpleConditionSchema = v.object({
  type: v.literal('simple'),
  op: simpleOperatorSchema,
  field: selectorSchema,
  value: v.object({
    type: v.literal('value'),
    value: v.union(primitiveSchema, primitiveArraySchema),
  }),
});

// The following ensures SimpleCondition and simpleConditionSchema
// are kept in sync (each type satisfies the other).
(t: SimpleCondition, inferredT: v.Infer<typeof simpleConditionSchema>) => {
  t satisfies v.Infer<typeof simpleConditionSchema>;
  inferredT satisfies SimpleCondition;
};
