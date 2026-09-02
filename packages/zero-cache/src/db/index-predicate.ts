import {assert} from '../../../shared/src/asserts.ts';
import {id, lit} from '../types/sql.ts';
import type {IndexPredicate, IndexPredicateValue} from './specs.ts';

const CANONICAL_INTEGER_RE = /^-?(?:0|[1-9]\d*)$/;

export function liteIndexPredicateSQL(predicate: IndexPredicate): string {
  switch (predicate.type) {
    case 'and':
    case 'or':
      assert(predicate.conditions.length > 0, 'empty index predicate');
      return `(${predicate.conditions
        .map(liteIndexPredicateSQL)
        .join(` ${predicate.type.toUpperCase()} `)})`;
    case 'not':
      return `(NOT ${liteIndexPredicateSQL(predicate.condition)})`;
    case 'null-test':
      return `${id(predicate.column)} ${predicate.op}`;
    case 'comparison':
      return `${id(predicate.column)} ${predicate.op} ${predicateValueSQL(
        predicate.value,
      )}`;
  }
}

function predicateValueSQL(value: IndexPredicateValue): string {
  switch (value.type) {
    case 'boolean':
      return value.value ? '1' : '0';
    case 'integer':
      // The PostgreSQL translator only constructs canonical decimal integers.
      assert(
        CANONICAL_INTEGER_RE.test(value.value),
        'invalid integer predicate',
      );
      return value.value;
    case 'string':
      return lit(value.value);
  }
}

export function mapIndexPredicateColumns(
  predicate: IndexPredicate,
  map: (column: string) => string,
): IndexPredicate {
  switch (predicate.type) {
    case 'and':
    case 'or':
      return {
        ...predicate,
        conditions: predicate.conditions.map(condition =>
          mapIndexPredicateColumns(condition, map),
        ),
      };
    case 'not':
      return {
        ...predicate,
        condition: mapIndexPredicateColumns(predicate.condition, map),
      };
    case 'null-test':
    case 'comparison':
      return {...predicate, column: map(predicate.column)};
  }
}
