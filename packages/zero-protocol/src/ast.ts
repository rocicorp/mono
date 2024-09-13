/**
 * Wire-format representation of the zql AST interface.
 *
 * `v.Type<...>` types are explicitly declared to facilitate Typescript verification
 * that the schemas satisfy the zql type definitions. (Incidentally, explicit types
 * are also required for recursive schema definitions.)
 */

import * as v from 'shared/src/valita.js';
import {CorrelatedSubQuery} from 'zql/src/zql/ast/ast.js';

function readonly<T>(t: v.Type<T>): v.Type<Readonly<T>> {
  return t as v.Type<Readonly<T>>;
}

export const selectorSchema = v.string();

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

export const equalityOpsSchema = v.union(v.literal('='), v.literal('!='));

export const orderOpsSchema = v.union(
  v.literal('<'),
  v.literal('>'),
  v.literal('<='),
  v.literal('>='),
);

export const likeOpsSchema = v.union(
  v.literal('LIKE'),
  v.literal('NOT LIKE'),
  v.literal('ILIKE'),
  v.literal('NOT ILIKE'),
);

export const inOpsSchema = v.union(v.literal('IN'), v.literal('NOT IN'));

export const simpleOperatorSchema = v.union(
  equalityOpsSchema,
  orderOpsSchema,
  likeOpsSchema,
  inOpsSchema,
);

export const conditionSchema = v.object({
  type: v.literal('simple'),
  op: simpleOperatorSchema,
  field: selectorSchema,
  value: v.union(
    v.string(),
    v.number(),
    v.boolean(),
    readonly(v.array(v.union(v.string(), v.number(), v.boolean()))),
  ),
});

// Split out so that its inferred type can be checked against
// Omit<CorrelatedSubQuery, 'correlation'> in ast-type-test.ts.
// The mutually-recursive reference of the 'other' field to astSchema
// is the only thing added in v.lazy.  The v.lazy is necessary due to the
// mutually-recursive types, but v.lazy prevents inference of the resulting
// type.
export const correlatedSubquerySchemaOmitSubquery = v.object({
  correlation: v.object({
    parentField: v.string(),
    childField: v.string(),
    op: v.literal('='),
  }),
  hidden: v.boolean().optional(),
});

export const correlatedSubquerySchema: v.Type<CorrelatedSubQuery> =
  correlatedSubquerySchemaOmitSubquery.extend({
    subquery: v.lazy(() => astSchema),
  });

export const astSchema = v.object({
  schema: v.string().optional(),
  table: v.string(),
  alias: v.string().optional(),
  where: readonly(v.array(conditionSchema)).optional(),
  related: readonly(v.array(correlatedSubquerySchema)).optional(),
  limit: v.number().optional(),
  orderBy: orderingSchema.optional(),
  start: v
    .object({
      row: v.record(
        v.union(v.string(), v.number(), v.boolean(), v.null(), v.undefined()),
      ),
      exclusive: v.boolean(),
    })
    .optional(),
});
