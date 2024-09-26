import {assert} from 'shared/src/asserts.js';
import type {JSONValue} from 'shared/src/json.js';
import {must} from 'shared/src/must.js';
import type {AST, Ordering} from '../ast/ast.js';
import type {Row} from '../ivm/data.js';
import {Filter} from '../ivm/filter.js';
import {Join} from '../ivm/join.js';
import type {Input, Storage} from '../ivm/operator.js';
import type {PrimaryKey} from '../ivm/schema.js';
import {Skip} from '../ivm/skip.js';
import type {Source} from '../ivm/source.js';
import {Take} from '../ivm/take.js';
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
  getSource(tableName: string): Source;

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
 *     console.log([...this.#input.hydrate()]);
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
  return buildPipelineInternal(ast, delegate, staticQueryParameters);
}

function buildPipelineInternal(
  ast: AST,
  delegate: BuilderDelegate,
  staticQueryParameters: StaticQueryParameters | undefined,
  partitionKey?: string | undefined,
): Input {
  const source = delegate.getSource(ast.table);
  const conn = source.connect(must(ast.orderBy), ast.where ?? []);
  let end: Input = conn;
  const {appliedFilters} = conn;

  if (ast.start) {
    end = new Skip(end, ast.start);
  }

  if (ast.where) {
    for (const condition of ast.where) {
      end = new Filter(
        end,
        appliedFilters ? 'push-only' : 'all',
        createPredicate(condition, staticQueryParameters),
      );
    }
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
