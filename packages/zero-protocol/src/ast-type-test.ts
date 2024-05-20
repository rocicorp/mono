import type {
  AST,
  Aggregate,
  Aggregation,
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
import type {
  aggregateSchema,
  aggregationSchema,
  astSchema,
  conjunctionSchema,
  equalityOpsSchema,
  inOpsSchema,
  joinOmitOther,
  likeOpsSchema,
  orderOpsSchema,
  orderingSchema,
  primitiveArraySchema,
  primitiveSchema,
  selectorSchema,
  setOpsSchema,
  simpleConditionSchema,
  simpleOperatorSchema,
} from './ast.js';
import type * as v from 'shared/src/valita.js';

/**
 * The following ensures the AST schemas in zero-protocol are in sync
 * with the AST types from '@rocicorp/zql/src/zql/ast/ast.js'.
 */
(t: Selector, inferredT: v.Infer<typeof selectorSchema>) => {
  t satisfies v.Infer<typeof selectorSchema>;
  inferredT satisfies Selector;
};

(t: Ordering, inferredT: v.Infer<typeof orderingSchema>) => {
  t satisfies v.Infer<typeof orderingSchema>;
  inferredT satisfies Ordering;
};

(t: Primitive, inferredT: v.Infer<typeof primitiveSchema>) => {
  t satisfies v.Infer<typeof primitiveSchema>;
  inferredT satisfies Primitive;
};

(t: PrimitiveArray, inferredT: v.Infer<typeof primitiveArraySchema>) => {
  t satisfies v.Infer<typeof primitiveArraySchema>;
  inferredT satisfies PrimitiveArray;
};

(t: Aggregate, inferredT: v.Infer<typeof aggregateSchema>) => {
  t satisfies v.Infer<typeof aggregateSchema>;
  inferredT satisfies Aggregate;
};

(t: Aggregation, inferredT: v.Infer<typeof aggregationSchema>) => {
  t satisfies v.Infer<typeof aggregationSchema>;
  inferredT satisfies Aggregation;
};

(t: Omit<Join, 'other'>, inferredT: v.Infer<typeof joinOmitOther>) => {
  t satisfies v.Infer<typeof joinOmitOther>;
  inferredT satisfies Omit<Join, 'other'>;
};

(t: Conjunction, inferredT: v.Infer<typeof conjunctionSchema>) => {
  t satisfies v.Infer<typeof conjunctionSchema>;
  inferredT satisfies Conjunction;
};

(t: AST, inferredT: v.Infer<typeof astSchema>) => {
  t satisfies v.Infer<typeof astSchema>;
  inferredT satisfies AST;
};

(t: EqualityOps, inferredT: v.Infer<typeof equalityOpsSchema>) => {
  t satisfies v.Infer<typeof equalityOpsSchema>;
  inferredT satisfies EqualityOps;
};

(t: OrderOps, inferredT: v.Infer<typeof orderOpsSchema>) => {
  t satisfies v.Infer<typeof orderOpsSchema>;
  inferredT satisfies OrderOps;
};

(t: InOps, inferredT: v.Infer<typeof inOpsSchema>) => {
  t satisfies v.Infer<typeof inOpsSchema>;
  inferredT satisfies InOps;
};

(t: LikeOps, inferredT: v.Infer<typeof likeOpsSchema>) => {
  t satisfies v.Infer<typeof likeOpsSchema>;
  inferredT satisfies LikeOps;
};

(t: SetOps, inferredT: v.Infer<typeof setOpsSchema>) => {
  t satisfies v.Infer<typeof setOpsSchema>;
  inferredT satisfies SetOps;
};

(t: SimpleOperator, inferredT: v.Infer<typeof simpleOperatorSchema>) => {
  t satisfies v.Infer<typeof simpleOperatorSchema>;
  inferredT satisfies SimpleOperator;
};

(t: SimpleCondition, inferredT: v.Infer<typeof simpleConditionSchema>) => {
  t satisfies v.Infer<typeof simpleConditionSchema>;
  inferredT satisfies SimpleCondition;
};
