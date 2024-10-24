import {assert} from '../../../../shared/src/asserts.js';
import type {JSONValue} from '../../../../shared/src/json.js';
import {must} from '../../../../shared/src/must.js';
import type {
  AST,
  Condition,
  Conjunction,
  Disjunction,
  LiteralValue,
  Ordering,
  Parameter,
  SimpleCondition,
  ValuePosition,
} from '../../../../zero-protocol/src/ast.js';
import type {Row} from '../../../../zero-protocol/src/data.js';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.js';
import {Filter} from '../ivm/filter.js';
import {Join} from '../ivm/join.js';
import type {Input, Storage} from '../ivm/operator.js';
import {Skip} from '../ivm/skip.js';
import type {Source} from '../ivm/source.js';
import {Take} from '../ivm/take.js';
import {MissingParameterError} from './error.js';
import {createPredicate} from './filter.js';

export type StaticQueryParameters = {
  authData: Record<string, JSONValue>;
  preMutationRow: Row | undefined;
};

/**
 * Interface required of caller to buildPipeline. Connects to constructed
 * pipeline to delegate environment to provide sources and storage.
 */
export interface BuilderDelegate {
  /**
   * Called once for each source needed by the AST.
   * Might be called multiple times with same tableName. It is OK to return
   * same storage instance in that case.
   */
  getSource(tableName: string): Source | undefined;

  /**
   * Called once for each operator that requires storage. Should return a new
   * unique storage object for each call.
   */
  createStorage(): Storage;
}

/**
 * Builds a pipeline from an AST. Caller must provide a delegate to create source
 * and storage interfaces as necessary.
 *
 * Usage:
 *
 * ```ts
 * class MySink implements Output {
 *   readonly #input: Input;
 *
 *   constructor(input: Input) {
 *     this.#input = input;
 *     this.#input.setOutput(this);
 *   }
 *
 *   push(change: Change, _: Operator) {
 *     console.log(change);
 *   }
 * }
 *
 * const input = buildPipeline(ast, myDelegate);
 * const sink = new MySink(input);
 * ```
 */
export function buildPipeline(
  ast: AST,
  delegate: BuilderDelegate,
  staticQueryParameters: StaticQueryParameters | undefined,
): Input {
  return buildPipelineInternal(
    bindStaticParameters(ast, staticQueryParameters),
    delegate,
    staticQueryParameters,
  );
}

export function bindStaticParameters(
  ast: AST,
  staticQueryParameters: StaticQueryParameters | undefined,
) {
  const visit = (node: AST): AST => {
    if (node.where) {
      return {
        ...node,
        where: bindCondition(node.where),
        related: node.related?.map(sq => ({
          ...sq,
          subquery: visit(sq.subquery),
        })),
      };
    }
    return node;
  };

  function bindCondition(condition: Condition): Condition {
    return condition.type === 'simple'
      ? {
          ...condition,
          value: bindValue(condition.value),
        }
      : {
          ...condition,
          conditions: condition.conditions.map(bindCondition),
        };
  }

  const bindValue = (value: ValuePosition): LiteralValue => {
    if (isParameter(value)) {
      const anchor = must(
        staticQueryParameters,
        'Static query params do not exist',
      )[value.anchor];
      assert(anchor !== undefined, `Missing parameter: ${value.anchor}`);
      const resolvedValue = anchor[value.field];
      // eslint-disable-next-line eqeqeq
      if (resolvedValue == null) {
        throw new MissingParameterError();
      }
      return resolvedValue as LiteralValue;
    }
    return value;
  };

  return visit(ast);
}

function isParameter(value: unknown): value is Parameter {
  return typeof value === 'object' && value !== null && 'type' in value;
}

function buildPipelineInternal(
  ast: AST,
  delegate: BuilderDelegate,
  staticQueryParameters: StaticQueryParameters | undefined,
  partitionKey?: string | undefined,
): Input {
  const source = delegate.getSource(ast.table);
  if (!source) {
    throw new Error(`Source not found: ${ast.table}`);
  }
  const conn = source.connect(must(ast.orderBy), ast.where);
  let end: Input = conn;
  const {appliedFilters} = conn;

  if (ast.start) {
    end = new Skip(end, ast.start);
  }

  if (ast.where) {
    end = applyWhere(end, ast.where, appliedFilters);
  }

  if (ast.limit) {
    end = new Take(end, delegate.createStorage(), ast.limit, partitionKey);
  }

  if (ast.related) {
    for (const sq of ast.related) {
      assert(sq.subquery.alias, 'Subquery must have an alias');
      const child = buildPipelineInternal(
        sq.subquery,
        delegate,
        staticQueryParameters,
        sq.correlation.childField,
      );
      end = new Join({
        parent: end,
        child,
        storage: delegate.createStorage(),
        parentKey: sq.correlation.parentField,
        childKey: sq.correlation.childField,
        relationshipName: sq.subquery.alias,
        hidden: sq.hidden ?? false,
      });
    }
  }

  return end;
}

function applyWhere(
  input: Input,
  condition: Condition,
  // Remove `appliedFilter`
  // Each branch can `fetch` with different filters from the same source.
  // Or we do the union of queries approach and retain this `appliedFilters` and `sourceConnect` behavior.
  // Downside of that being unbounded memory usage.
  appliedFilters: boolean,
): Input {
  switch (condition.type) {
    case 'and':
      return applyAnd(input, condition, appliedFilters);
    case 'or':
      return applyOr(input, condition, appliedFilters);
    default:
      return applySimpleCondition(input, condition, appliedFilters);
  }
}

function applyAnd(
  input: Input,
  condition: Conjunction,
  appliedFilters: boolean,
) {
  for (const subCondition of condition.conditions) {
    input = applyWhere(input, subCondition, appliedFilters);
  }
  return input;
}

function applyOr(
  _input: Input,
  _condition: Disjunction,
  _appliedFilters: boolean,
): Input {
  throw new Error('OR is not yet implemented');
}

function applySimpleCondition(
  input: Input,
  condition: SimpleCondition,
  appliedFilters: boolean,
): Input {
  return new Filter(
    input,
    appliedFilters ? 'push-only' : 'all',
    createPredicate(condition),
  );
}

export function assertOrderingIncludesPK(
  ordering: Ordering,
  pk: PrimaryKey,
): void {
  const orderingFields = ordering.map(([field]) => field);
  const missingFields = pk.filter(pkField => !orderingFields.includes(pkField));

  if (missingFields.length > 0) {
    throw new Error(
      `Ordering must include all primary key fields. Missing: ${missingFields.join(
        ', ',
      )}`,
    );
  }
}
