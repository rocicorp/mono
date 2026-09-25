import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {AST, Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import {PROTOCOL_VERSION} from '../../../zero-protocol/src/protocol-version.ts';

export function completeOrdering(
  ast: AST,
  getPrimaryKey: (tableName: string) => PrimaryKey,
  protocolVersion: number = PROTOCOL_VERSION,
): AST {
  const primaryKey = must(getPrimaryKey(ast.table));
  return {
    ...ast,
    ...(ast.related
      ? {
          related: ast.related?.map(r => ({
            ...r,
            subquery: completeOrdering(
              r.subquery,
              getPrimaryKey,
              protocolVersion,
            ),
          })),
        }
      : undefined),
    ...(ast.where
      ? {
          where: completeOrderingInCondition(
            ast.where,
            getPrimaryKey,
            protocolVersion,
          ),
        }
      : undefined),
    orderBy: addPrimaryKeys(primaryKey, ast.orderBy, protocolVersion),
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
  protocolVersion: number = PROTOCOL_VERSION,
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
        subquery: completeOrdering(
          condition.related.subquery,
          getPrimaryKey,
          protocolVersion,
        ),
      },
    };
  }
  condition.type satisfies 'and' | 'or';
  return {
    ...condition,
    conditions: condition.conditions.map(c =>
      completeOrderingInCondition(c, getPrimaryKey, protocolVersion),
    ),
  };
}

function addPrimaryKeys(
  primaryKey: PrimaryKey,
  orderBy: Ordering | undefined,
  protocolVersion: number = PROTOCOL_VERSION,
): Ordering {
  orderBy = orderBy ?? [];
  const primaryKeysToAdd = new Set(primaryKey);

  for (const [field] of orderBy) {
    primaryKeysToAdd.delete(field);
  }

  if (primaryKeysToAdd.size === 0) {
    return orderBy;
  }

  const trailingDirection: 'asc' | 'desc' =
    protocolVersion >= 54 ? (orderBy.at(-1)?.[1] ?? 'asc') : 'asc';

  return [
    ...orderBy,
    ...Array.from(
      primaryKeysToAdd,
      key => [key, trailingDirection] as [string, 'asc' | 'desc'],
    ),
  ];
}
