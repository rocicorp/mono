/**
 * Wire-format representation of the zql AST interface.
 *
 * `v.Type<...>` types are explicitly declared to facilitate Typescript verification
 * that the schemas satisfy the zql type definitions. (Incidentally, explicit types
 * are also required for recursive schema definitions.)
 */

import * as v from 'shared/src/valita.js';

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

export const correlatedSubquerySchema: v.Type<{
  correlation: {
    parentField: string;
    childField: string;
    op: '=';
  };
  hidden?: boolean | undefined;
  subquery: AST;
}> = v.object({
  correlation: v.object({
    parentField: v.string(),
    childField: v.string(),
    op: v.literal('='),
  }),
  hidden: v.boolean().optional(),
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
});

type AST = v.Infer<typeof astSchema>;
