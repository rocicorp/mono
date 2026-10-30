import {
  parseSync,
  type ArrowFunctionExpression,
  type Expression,
  type Node,
  type ObjectExpression,
} from 'oxc-parser';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {ExpressionBuilder} from '../../zql/src/query/expression.ts';
import type {Query} from '../../zql/src/query/query.ts';

/**
 * Evaluates a ZQL query written in chain form, e.g.
 * `issue.where('open', true).related('comments', q => q.limit(10))`.
 *
 * The text is parsed with oxc and the resulting JavaScript AST is evaluated
 * directly. It is never run as JavaScript. Only these constructs are
 * accepted:
 *
 * - identifiers: table names (looked up on `root`), arrow function
 *   parameters, and `undefined`
 * - calls to the query and expression builder methods in
 *   {@link ALLOWED_METHODS}, either as `x.method(...)` or through a
 *   destructured parameter such as `({cmp, or}) => or(cmp(...))`
 * - string, number, boolean and null literals, negative numbers, and
 *   template literals without substitutions
 * - array and object literals
 * - arrow functions with an expression body whose parameters are
 *   identifiers or `{a, b}` patterns of allowed method names
 *
 * Anything else, including access to any other property, is rejected.
 */
export function evaluateZQLChain(text: string, root: object): unknown {
  const {program, errors} = parseSync('query.js', text, {
    lang: 'js',
    sourceType: 'script',
  });
  if (errors.length > 0) {
    const [error] = errors;
    const pos = error.labels[0]?.start;
    throw new SyntaxError(
      pos === undefined ? error.message : `${error.message} at position ${pos}`,
    );
  }
  const [statement, ...rest] = program.body;
  if (statement?.type !== 'ExpressionStatement' || rest.length > 0) {
    throw new SyntaxError('Expected a single query expression');
  }
  return new Evaluator(text, root).evaluate(statement.expression, new Map());
}

// Every string-keyed member of Query and ExpressionBuilder is listed here, so
// adding or removing one fails type checking until it is classified as
// allowed or not. Symbol keys can't be written in the query text.
type MethodName<T> = Extract<keyof T, string>;

const QUERY_METHODS = {
  where: true,
  whereExists: true,
  related: true,
  orderBy: true,
  limit: true,
  one: true,
  start: true,
  count: true,
  sum: true,
  avg: true,
  min: true,
  max: true,
  distinct: true,
  groupBy: true,
  run: false,
  preload: false,
  materialize: false,
} satisfies Record<MethodName<Query<string, Schema>>, boolean>;

const EXPRESSION_BUILDER_METHODS = {
  cmp: true,
  cmpLit: true,
  and: true,
  or: true,
  not: true,
  exists: true,
  eb: true,
  json: true,
} satisfies Record<MethodName<ExpressionBuilder<string, Schema>>, boolean>;

const ALLOWED_METHODS: ReadonlySet<string> = new Set(
  Object.entries({...QUERY_METHODS, ...EXPRESSION_BUILDER_METHODS})
    .filter(([, allowed]) => allowed)
    .map(([name]) => name),
);

type Scope = ReadonlyMap<string, unknown>;

class Evaluator {
  readonly #text: string;
  readonly #root: object;

  constructor(text: string, root: object) {
    this.#text = text;
    this.#root = root;
  }

  #reject(node: Node, what: string): never {
    const source = this.#text.slice(node.start, node.end);
    throw new SyntaxError(
      `${what} is not allowed: \`${source}\` at position ${node.start}`,
    );
  }

  evaluate(node: Expression, scope: Scope): unknown {
    switch (node.type) {
      case 'ParenthesizedExpression':
        return this.evaluate(node.expression, scope);
      case 'Literal':
        if (
          (node as {regex?: unknown}).regex ||
          (node as {bigint?: unknown}).bigint
        ) {
          this.#reject(node, 'Literal');
        }
        return node.value;
      case 'TemplateLiteral':
        if (node.expressions.length > 0) {
          this.#reject(node, 'Template substitution');
        }
        return node.quasis[0].value.cooked;
      case 'UnaryExpression':
        if (
          node.operator === '-' &&
          node.argument.type === 'Literal' &&
          typeof node.argument.value === 'number'
        ) {
          return -node.argument.value;
        }
        return this.#reject(node, 'Operator');
      case 'Identifier':
        return this.#lookup(node, node.name, scope);
      case 'ArrayExpression':
        return node.elements.map(e => {
          if (e === null || e.type === 'SpreadElement') {
            return this.#reject(node, 'Array hole or spread');
          }
          return this.evaluate(e, scope);
        });
      case 'ObjectExpression':
        return this.#evaluateObject(node, scope);
      case 'ArrowFunctionExpression':
        return this.#evaluateArrow(node, scope);
      case 'CallExpression':
        return this.#evaluateCall(node, scope);
      default:
        return this.#reject(node, node.type);
    }
  }

  #lookup(node: Node, name: string, scope: Scope): unknown {
    if (scope.has(name)) {
      return scope.get(name);
    }
    if (name === 'undefined') {
      return undefined;
    }
    if (!Object.hasOwn(this.#root, name)) {
      this.#reject(node, 'Unknown identifier');
    }
    return (this.#root as Record<string, unknown>)[name];
  }

  #evaluateObject(node: ObjectExpression, scope: Scope): unknown {
    const obj: Record<string, unknown> = {};
    for (const prop of node.properties) {
      if (
        prop.type !== 'Property' ||
        prop.kind !== 'init' ||
        prop.method ||
        prop.computed
      ) {
        return this.#reject(prop, 'Object member');
      }
      let key: string;
      if (prop.key.type === 'Identifier') {
        key = prop.key.name;
      } else if (
        prop.key.type === 'Literal' &&
        typeof prop.key.value === 'string'
      ) {
        key = prop.key.value;
      } else {
        return this.#reject(prop.key, 'Object key');
      }
      if (key === '__proto__') {
        return this.#reject(prop.key, 'Object key');
      }
      obj[key] = this.evaluate(prop.value as Expression, scope);
    }
    return obj;
  }

  #evaluateArrow(node: ArrowFunctionExpression, scope: Scope): unknown {
    if (!node.expression || node.async) {
      return this.#reject(node, 'Arrow function with a block body');
    }
    const body = node.body as Expression;
    const params = node.params.map(param => {
      if (param.type === 'Identifier') {
        return {name: param.name};
      }
      if (param.type !== 'ObjectPattern') {
        return this.#reject(param, 'Parameter');
      }
      return {
        names: param.properties.map(p => {
          if (
            p.type !== 'Property' ||
            p.computed ||
            p.value.type !== 'Identifier' ||
            !p.shorthand ||
            p.key.type !== 'Identifier' ||
            !ALLOWED_METHODS.has(p.key.name)
          ) {
            return this.#reject(p, 'Destructured parameter');
          }
          return p.key.name;
        }),
      };
    });
    return (...args: unknown[]) => {
      const inner = new Map(scope);
      params.forEach((param, i) => {
        if ('name' in param) {
          inner.set(param.name, args[i]);
        } else {
          const arg = args[i] as Record<string, unknown>;
          for (const name of param.names) {
            inner.set(name, arg[name]);
          }
        }
      });
      return this.evaluate(body, inner);
    };
  }

  #evaluateCall(
    node: Extract<Expression, {type: 'CallExpression'}>,
    scope: Scope,
  ): unknown {
    if (node.optional) {
      return this.#reject(node, 'Optional call');
    }
    const args = node.arguments.map(a => {
      if (a.type === 'SpreadElement') {
        return this.#reject(a, 'Spread');
      }
      return this.evaluate(a, scope);
    });
    const {callee} = node;
    let thisValue: unknown;
    let fn: unknown;
    if (callee.type === 'MemberExpression') {
      if (
        callee.computed ||
        callee.optional ||
        callee.property.type !== 'Identifier' ||
        !ALLOWED_METHODS.has(callee.property.name)
      ) {
        return this.#reject(callee, 'Method');
      }
      thisValue = this.evaluate(callee.object, scope);
      if (thisValue === null || typeof thisValue !== 'object') {
        return this.#reject(callee.object, 'Receiver');
      }
      fn = (thisValue as Record<string, unknown>)[callee.property.name];
    } else if (callee.type === 'Identifier' && scope.has(callee.name)) {
      fn = scope.get(callee.name);
    } else {
      return this.#reject(callee, 'Callee');
    }
    if (typeof fn !== 'function') {
      return this.#reject(callee, 'Non-function call');
    }
    return Reflect.apply(fn, thisValue, args);
  }
}
