import type * as v from 'shared/src/valita.js';
import type {
  AST,
  Condition,
  CorrelatedSubQuery,
  EqualityOps,
  LikeOps,
  OrderOps,
  Ordering,
  SimpleOperator,
} from 'zql/src/zql/ast/ast.js';
import type {
  astSchema,
  conditionSchema,
  correlatedSubquerySchema,
  equalityOpsSchema,
  likeOpsSchema,
  orderOpsSchema,
  orderingSchema,
  simpleOperatorSchema,
} from './ast.js';

(t: Ordering, inferredT: v.Infer<typeof orderingSchema>) => {
  t satisfies v.Infer<typeof orderingSchema>;
  inferredT satisfies Ordering;
};

(t: AST, inferredT: v.Infer<typeof astSchema>) => {
  t satisfies v.Infer<typeof astSchema>;
  inferredT satisfies AST;
};

(
  t: CorrelatedSubQuery,
  inferredT: v.Infer<typeof correlatedSubquerySchema>,
) => {
  t satisfies v.Infer<typeof correlatedSubquerySchema>;
  inferredT satisfies CorrelatedSubQuery;
};

(t: EqualityOps, inferredT: v.Infer<typeof equalityOpsSchema>) => {
  t satisfies v.Infer<typeof equalityOpsSchema>;
  inferredT satisfies EqualityOps;
};

(t: OrderOps, inferredT: v.Infer<typeof orderOpsSchema>) => {
  t satisfies v.Infer<typeof orderOpsSchema>;
  inferredT satisfies OrderOps;
};

(t: LikeOps, inferredT: v.Infer<typeof likeOpsSchema>) => {
  t satisfies v.Infer<typeof likeOpsSchema>;
  inferredT satisfies LikeOps;
};

(t: SimpleOperator, inferredT: v.Infer<typeof simpleOperatorSchema>) => {
  t satisfies v.Infer<typeof simpleOperatorSchema>;
  inferredT satisfies SimpleOperator;
};

(t: Condition, inferredT: v.Infer<typeof conditionSchema>) => {
  t satisfies v.Infer<typeof conditionSchema>;
  inferredT satisfies Condition;
};
