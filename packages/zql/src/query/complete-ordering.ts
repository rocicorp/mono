import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {AST, Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';

export function completeOrdering(
  ast: AST,
  getPrimaryKey: (tableName: string) => PrimaryKey,
): AST {
  const primaryKey = must(getPrimaryKey(ast.table));
  return {
    ...ast,
    ...(ast.related
      ? {
          related: ast.related?.map(r => ({
            ...r,
            subquery: completeOrdering(r.subquery, getPrimaryKey),
          })),
        }
      : undefined),
    ...(ast.where
      ? {
          where: completeOrderingInCondition(ast.where, getPrimaryKey),
        }
      : undefined),
    orderBy: addPrimaryKeys(primaryKey, ast.orderBy),
  };
}

export function assertOrderingIncludesPK(
  ordering: Ordering,
  pk: PrimaryKey,
): void {
  // oxlint-disable-next-line unicorn/prefer-set-has -- Array is more appropriate here for small collections
  const orderingFields = ordering.map(([field]) => field);
  const missingFields = pk.filter(pkField => !orderingFields.includes(pkField));

  assert(
    missingFields.length === 0,
    `Ordering must include all primary key fields. Missing: ${missingFields.join(
      ', ',
    )}.`,
  );
}

function completeOrderingInCondition<C extends Condition | undefined>(
  condition: C,
  getPrimaryKey: (tableName: string) => PrimaryKey,
): C {
  if (!condition) {
    return condition;
  }
  if (condition.type === 'simple') {
    return condition;
  }
  if (condition.type === 'correlatedSubquery') {
    return {
      ...condition,
      related: {
        ...condition.related,
        subquery: completeOrdering(condition.related.subquery, getPrimaryKey),
      },
    };
  }
  if (condition.type === 'scalarSubquery') {
    return {
      ...condition,
      subquery: completeOrdering(condition.subquery, getPrimaryKey),
    };
  }
  condition.type satisfies 'and' | 'or';
  return {
    ...condition,
    conditions: condition.conditions.map(c =>
      completeOrderingInCondition(c, getPrimaryKey),
    ),
  };
}

function addPrimaryKeys(
  primaryKey: PrimaryKey,
  orderBy: Ordering | undefined,
): Ordering {
  orderBy = orderBy ?? [];
  const primaryKeysToAdd = new Set(primaryKey);

  for (const [field] of orderBy) {
    primaryKeysToAdd.delete(field);
  }

  if (primaryKeysToAdd.size === 0) {
    return orderBy;
  }

  return [
    ...orderBy,
    ...[...primaryKeysToAdd].map(key => [key, 'asc'] as [string, 'asc']),
  ];
}
