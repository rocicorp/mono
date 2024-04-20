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
  SimpleCondition,
  SimpleOperator,
} from '@rocicorp/zql/src/zql/ast/ast.js';
import * as v from 'shared/src/valita.js';

export const orderingSchema: v.Type<Ordering> = v.tuple([
  v.array(v.string()),
  v.union(v.literal('asc'), v.literal('desc')),
]);

export const primitiveSchema: v.Type<Primitive> = v.union(
  v.string(),
  v.number(),
  v.boolean(),
  v.null(),
);

export const aggregateSchema: v.Type<Aggregate> = v.union(
  v.literal('sum'),
  v.literal('avg'),
  v.literal('min'),
  v.literal('max'),
  v.literal('array'),
  v.literal('count'),
);
export const aggregationSchema: v.Type<Aggregation> = v.object({
  field: v.string().optional(),
  alias: v.string(),
  aggregate: aggregateSchema,
});

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
    on: v.tuple([v.string(), v.string()]),
  }),
);

export const astSchema: v.Type<AST> = v.lazy(() =>
  v.object({
    table: v.string(),
    alias: v.string().optional(),
    select: v.array(v.tuple([v.string(), v.string()])).optional(),
    aggregate: v.array(aggregationSchema).optional(),
    where: conditionSchema.optional(),
    joins: v.array(joinSchema).optional(),
    groupBy: v.array(v.string()).optional(),
    orderBy: orderingSchema.optional(),
  }),
);

export const conditionSchema: v.Type<Condition> = v.lazy(() =>
  v.union(simpleConditionSchema, conjunctionSchema),
);

export const conjunctionSchema: v.Type<Conjunction> = v.lazy(() =>
  v.object({
    type: v.literal('conjunction'),
    op: v.union(v.literal('AND'), v.literal('OR')),
    conditions: v.array(conditionSchema),
  }),
);

export const equalityOpsSchema: v.Type<EqualityOps> = v.union(
  v.literal('='),
  v.literal('!='),
);

export const orderOpsSchema: v.Type<OrderOps> = v.union(
  v.literal('<'),
  v.literal('>'),
  v.literal('<='),
  v.literal('>='),
);

export const inOpsSchema: v.Type<InOps> = v.union(
  v.literal('IN'),
  v.literal('NOT IN'),
);

export const likeOpsSchema: v.Type<LikeOps> = v.union(
  v.literal('LIKE'),
  v.literal('NOT LIKE'),
  v.literal('ILIKE'),
  v.literal('NOT ILIKE'),
);

export const simpleOperatorSchema: v.Type<SimpleOperator> = v.union(
  equalityOpsSchema,
  orderOpsSchema,
  inOpsSchema,
  likeOpsSchema,
);

export const simpleConditionSchema: v.Type<SimpleCondition> = v.object({
  type: v.literal('simple'),
  op: simpleOperatorSchema,
  field: v.string(),
  value: v.object({
    type: v.literal('literal'),
    value: primitiveSchema,
  }),
});
