import type * as v from '../../shared/src/valita.js';
import type {
  AST,
  Condition,
  CorrelatedSubQuery,
  EqualityOps,
  LikeOps,
  OrderOps,
  Ordering,
  SimpleOperator,
  astSchema,
  conditionSchema,
  correlatedSubquerySchema,
  correlatedSubquerySchemaOmitSubquery,
  equalityOpsSchema,
  likeOpsSchema,
  orderOpsSchema,
  orderingSchema,
  simpleOperatorSchema,
} from './ast.js';

type MakeAllFieldsRequired<T> = {
  [K in keyof T]-?: MakeAllFieldsRequired<T[K]>;
};

(
  t: Omit<CorrelatedSubQuery, 'subquery'>,
  inferredT: v.Infer<typeof correlatedSubquerySchemaOmitSubquery>,
  tR: MakeAllFieldsRequired<Omit<CorrelatedSubQuery, 'subquery'>>,
  inferredTR: MakeAllFieldsRequired<
    v.Infer<typeof correlatedSubquerySchemaOmitSubquery>
  >,
) => {
  t satisfies v.Infer<typeof correlatedSubquerySchemaOmitSubquery>;
  inferredT satisfies Omit<CorrelatedSubQuery, 'subquery'>;

  inferredTR satisfies MakeAllFieldsRequired<
    Omit<CorrelatedSubQuery, 'subquery'>
  >;
  tR satisfies MakeAllFieldsRequired<
    v.Infer<typeof correlatedSubquerySchemaOmitSubquery>
  >;
};

(
  t: AST,
  inferredT: v.Infer<typeof astSchema>,
  tR: MakeAllFieldsRequired<AST>,
  inferredTR: MakeAllFieldsRequired<v.Infer<typeof astSchema>>,
) => {
  t satisfies v.Infer<typeof astSchema>;
  inferredT satisfies AST;

  inferredTR satisfies MakeAllFieldsRequired<AST>;
  tR satisfies MakeAllFieldsRequired<v.Infer<typeof astSchema>>;
};

(
  t: Condition,
  inferredT: v.Infer<typeof conditionSchema>,
  tR: MakeAllFieldsRequired<Condition>,
  inferredTR: MakeAllFieldsRequired<v.Infer<typeof conditionSchema>>,
) => {
  t satisfies v.Infer<typeof conditionSchema>;
  inferredT satisfies Condition;

  inferredTR satisfies MakeAllFieldsRequired<Condition>;
  tR satisfies MakeAllFieldsRequired<v.Infer<typeof conditionSchema>>;
};

(
  t: CorrelatedSubQuery,
  inferredT: v.Infer<typeof correlatedSubquerySchema>,
  tR: MakeAllFieldsRequired<CorrelatedSubQuery>,
  inferredTR: MakeAllFieldsRequired<v.Infer<typeof correlatedSubquerySchema>>,
) => {
  t satisfies v.Infer<typeof correlatedSubquerySchema>;
  inferredT satisfies CorrelatedSubQuery;

  inferredTR satisfies MakeAllFieldsRequired<CorrelatedSubQuery>;
  tR satisfies MakeAllFieldsRequired<v.Infer<typeof correlatedSubquerySchema>>;
};

(
  t: EqualityOps,
  inferredT: v.Infer<typeof equalityOpsSchema>,
  tR: MakeAllFieldsRequired<EqualityOps>,
  inferredTR: MakeAllFieldsRequired<v.Infer<typeof equalityOpsSchema>>,
) => {
  t satisfies v.Infer<typeof equalityOpsSchema>;
  inferredT satisfies EqualityOps;

  inferredTR satisfies MakeAllFieldsRequired<EqualityOps>;
  tR satisfies MakeAllFieldsRequired<v.Infer<typeof equalityOpsSchema>>;
};

(
  t: LikeOps,
  inferredT: v.Infer<typeof likeOpsSchema>,
  tR: MakeAllFieldsRequired<LikeOps>,
  inferredTR: MakeAllFieldsRequired<v.Infer<typeof likeOpsSchema>>,
) => {
  t satisfies v.Infer<typeof likeOpsSchema>;
  inferredT satisfies LikeOps;

  inferredTR satisfies MakeAllFieldsRequired<LikeOps>;
  tR satisfies MakeAllFieldsRequired<v.Infer<typeof likeOpsSchema>>;
};

(
  t: OrderOps,
  inferredT: v.Infer<typeof orderOpsSchema>,
  tR: MakeAllFieldsRequired<OrderOps>,
  inferredTR: MakeAllFieldsRequired<v.Infer<typeof orderOpsSchema>>,
) => {
  t satisfies v.Infer<typeof orderOpsSchema>;
  inferredT satisfies OrderOps;

  inferredTR satisfies MakeAllFieldsRequired<OrderOps>;
  tR satisfies MakeAllFieldsRequired<v.Infer<typeof orderOpsSchema>>;
};

(
  t: Ordering,
  inferredT: v.Infer<typeof orderingSchema>,
  tR: MakeAllFieldsRequired<Ordering>,
  inferredTR: MakeAllFieldsRequired<v.Infer<typeof orderingSchema>>,
) => {
  t satisfies v.Infer<typeof orderingSchema>;
  inferredT satisfies Ordering;

  inferredTR satisfies MakeAllFieldsRequired<Ordering>;
  tR satisfies MakeAllFieldsRequired<v.Infer<typeof orderingSchema>>;
};

(
  t: SimpleOperator,
  inferredT: v.Infer<typeof simpleOperatorSchema>,
  tR: MakeAllFieldsRequired<SimpleOperator>,
  inferredTR: MakeAllFieldsRequired<v.Infer<typeof simpleOperatorSchema>>,
) => {
  t satisfies v.Infer<typeof simpleOperatorSchema>;
  inferredT satisfies SimpleOperator;

  inferredTR satisfies MakeAllFieldsRequired<SimpleOperator>;
  tR satisfies MakeAllFieldsRequired<v.Infer<typeof simpleOperatorSchema>>;
};
