import type {AST, Condition} from '../../../zero-protocol/src/ast.js';
import type {AuthorizationConfig} from '../../../zero-schema/src/compiled-authorization.js';
import {dnf} from '../../../zql/src/query/dnf.js';

/**
 * For a given AST, apply the read-auth rules.
 */
export function transformQuery(
  query: AST,
  auth: AuthorizationConfig,
): AST | undefined {
  const rowSelectRules = auth[query.table]?.row?.select;

  if (rowSelectRules && rowSelectRules.length === 0) {
    // The table cannot be read, ever. Nuke the query since
    // there is nothing for it to do.
    return undefined;
  }

  const updatedWhere = addRulesToWhere(
    query.where ? augmentCondition(query.where, auth) : undefined,
    rowSelectRules,
  );
  return {
    ...query,
    where: updatedWhere ? dnf(updatedWhere) : undefined,
    related: query.related
      ?.map(sq => {
        const subquery = transformQuery(sq.subquery, auth);
        if (subquery) {
          return {
            ...sq,
            subquery,
          };
        }
        return undefined;
      })
      .filter(q => q !== undefined),
  };
}

function addRulesToWhere(
  where: Condition | undefined,
  rowSelectRules: ['allow', Condition][] | undefined,
): Condition | undefined {
  if (rowSelectRules === undefined) {
    return where;
  }

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
function augmentCondition(
  cond: Condition,
  auth: AuthorizationConfig,
): Condition {
  switch (cond.type) {
    case 'simple':
      return cond;
    case 'and':
    case 'or':
      return {
        ...cond,
        conditions: cond.conditions.map(c => augmentCondition(c, auth)),
      };
    case 'correlatedSubquery': {
      const query = transformQuery(cond.related.subquery, auth);
      const replacement = query
        ? {
            ...cond,
            related: {
              ...cond.related,
              subquery: query,
            },
          }
        : undefined;
      switch (cond.op) {
        case 'EXISTS':
          return replacement
            ? replacement
            : {
                type: 'simple',
                left: {type: 'literal', value: true},
                op: '=',
                right: {type: 'literal', value: false},
              };
        case 'NOT EXISTS':
          return replacement
            ? replacement
            : {
                type: 'simple',
                left: {type: 'literal', value: true},
                op: '=',
                right: {type: 'literal', value: true},
              };
      }
    }
  }
}
