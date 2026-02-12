import type {LogContext} from '@rocicorp/logger';
import type {JSONValue} from '../../../shared/src/json.ts';
import type {AST, Condition} from '../../../zero-protocol/src/ast.ts';
import {hashOfAST} from '../../../zero-protocol/src/query-hash.ts';
import type {PermissionsConfig} from '../../../zero-schema/src/compiled-permissions.ts';
import {bindStaticParameters} from '../../../zql/src/builder/builder.ts';
import {simplifyCondition} from '../../../zql/src/query/expression.ts';
import type {JWTAuth} from './auth.ts';

export type TransformedAndHashed = {
  id: string;
  transformedAst: AST;
  transformationHash: string;
};
/**
 * Adds permission rules to the given query so it only returns rows that the
 * user is allowed to read.
 *
 * If the returned query is `undefined` that means that user cannot run
 * the query at all. This is only the case if we can infer that all rows
 * would be excluded without running the query.
 * E.g., the user is trying to query a table that is not readable.
 */
export function transformAndHashQuery(
  lc: LogContext,
  id: string,
  query: AST,
  permissionRules: PermissionsConfig,
  auth: JWTAuth | undefined,
  internalQuery: boolean | null | undefined,
): TransformedAndHashed {
  const transformed = internalQuery
    ? query // application permissions do not apply to internal queries
    : transformQuery(lc, query, permissionRules, auth);
  return {
    id,
    transformedAst: transformed,
    transformationHash: hashOfAST(transformed),
  };
}

/**
 * For a given AST, apply the read-auth rules and bind static auth data.
 */
export function transformQuery(
  lc: LogContext,
  query: AST,
  permissionRules: PermissionsConfig,
  auth: JWTAuth | undefined,
): AST {
  const queryWithPermissions = transformQueryInternal(
    lc,
    query,
    permissionRules,
  );
  return bindStaticParameters(queryWithPermissions, {
    authData: auth ? (auth.decoded as Record<string, JSONValue>) : {},
  });
}

function transformQueryInternal(
  lc: LogContext,
  query: AST,
  permissionRules: PermissionsConfig,
): AST {
  let rowSelectRules = permissionRules?.tables?.[query.table]?.row?.select;

  if (!rowSelectRules || rowSelectRules.length === 0) {
    // If there are no rules, we default to not allowing any rows to be selected.
    lc.warn?.(
      "No permission rules found for table '" +
        query.table +
        "'. No rows will be returned. Use ANYONE_CAN to allow all users to access all rows.",
    );
    rowSelectRules = [
      [
        'allow',
        {
          type: 'or',
          conditions: [],
        },
      ],
    ];
  }

  const updatedWhere = addRulesToWhere(
    query.where
      ? transformCondition(lc, query.where, permissionRules)
      : undefined,
    rowSelectRules,
  );
  return {
    ...query,
    where: simplifyCondition(updatedWhere),
    related: query.related?.map(sq => {
      const subquery = transformQueryInternal(lc, sq.subquery, permissionRules);
      return {
        ...sq,
        subquery,
      };
    }),
  };
}

function addRulesToWhere(
  where: Condition | undefined,
  rowSelectRules: ['allow', Condition][],
): Condition {
  return {
    type: 'and',
    conditions: [
      ...(where ? [where] : []),
      {
        type: 'or',
        conditions: rowSelectRules.map(([_, condition]) => condition),
      },
    ],
  };
}

// We must augment conditions so we do not provide an oracle to users.
// E.g.,
// `issue.whereExists('secret', s => s.where('value', 'sdf'))`
// Not applying read policies to subqueries in the where position
// would allow users to infer the existence of rows, and their contents,
// that they cannot read.
function transformCondition(
  lc: LogContext,
  cond: Condition,
  auth: PermissionsConfig,
): Condition {
  switch (cond.type) {
    case 'simple':
      return cond;
    case 'and':
    case 'or':
      return {
        ...cond,
        conditions: cond.conditions.map(c => transformCondition(lc, c, auth)),
      };
    case 'correlatedSubquery': {
      const query = transformQueryInternal(lc, cond.related.subquery, auth);
      return {
        ...cond,
        related: {
          ...cond.related,
          subquery: query,
        },
      };
    }
    case 'scalarSubquery': {
      const query = transformQueryInternal(lc, cond.subquery, auth);
      return {
        ...cond,
        subquery: query,
      };
    }
  }
}
