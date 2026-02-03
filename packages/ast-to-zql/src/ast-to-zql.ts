import {unreachable} from '../../shared/src/asserts.ts';
import {must} from '../../shared/src/must.ts';
import type {
  AST,
  Condition,
  Conjunction,
  CorrelatedSubquery,
  CorrelatedSubqueryCondition,
  Disjunction,
  LiteralReference,
  Ordering,
  Parameter,
  SimpleCondition,
  ValuePosition,
} from '../../zero-protocol/src/ast.ts';
import {SUBQ_PREFIX} from '../../zero-protocol/src/ast.ts';

/**
 * Converts an AST to the equivalent query builder code.
 * This is useful for debugging and understanding queries.
 *
 * @example
 * ```
 * const ast = query.issue.where('id', '=', 123)[astSymbol];
 * console.log(astToZQL(ast)); // outputs: .where('id', '=', 123)
 * ```
 */
export function astToZQL(ast: AST): string {
  let code = '';

  // Handle where conditions
  if (ast.where) {
    code += transformCondition(ast.where, '.where', new Set());
  }

  // Handle related subqueries
  if (ast.related && ast.related.length > 0) {
    for (const related of ast.related) {
      if (related.hidden) {
        const nestedRelated = related.subquery.related?.[0];
        if (nestedRelated) {
          code += transformRelated(nestedRelated);
        }
      } else {
        code += transformRelated(related);
      }
    }
  }

  // Handle orderBy
  if (ast.orderBy && ast.orderBy.length > 0) {
    code += transformOrder(ast.orderBy);
  }

  // Handle limit
  if (ast.limit !== undefined) {
    code += `.limit(${ast.limit})`;
  }

  // Handle start
  if (ast.start) {
    const {row, exclusive} = ast.start;
    code += `.start(${JSON.stringify(row)}${
      exclusive ? '' : ', { inclusive: true }'
    })`;
  }

  return code;
}

type Args = Set<string>;

type Prefix = '.where' | 'cmp';

function transformCondition(
  condition: Condition,
  prefix: Prefix,
  args: Args,
): string {
  switch (condition.type) {
    case 'simple':
      return transformSimpleCondition(condition, prefix);
    case 'and':
    case 'or':
      return transformLogicalCondition(condition, prefix, args);
    case 'correlatedSubquery':
      return transformExistsCondition(condition, prefix, args);
    case 'scalarSubquery':
      return `${prefix}Scalar('${condition.field[0]}', '${condition.op}', ...)`;
    default:
      unreachable(condition);
  }
}

function transformSimpleCondition(
  condition: SimpleCondition,
  prefix: Prefix,
): string {
  const {left, op, right} = condition;

  const leftCode = transformValuePosition(left);
  const rightCode = transformValuePosition(right);

  // Handle the shorthand form for equals
  if (op === '=') {
    return `${prefix}(${leftCode}, ${rightCode})`;
  }

  return `${prefix}(${leftCode}, '${op}', ${rightCode})`;
}

function transformLogicalCondition(
  condition: Conjunction | Disjunction,
  prefix: Prefix,
  args: Args,
): string {
  const {type, conditions} = condition;

  // For single condition, no need for logical operator
  if (conditions.length === 1) {
    return transformCondition(conditions[0], prefix, args);
  }

  // Generate multiple where calls for top-level AND conditions
  if (type === 'and') {
    const parts = conditions.map(c => transformCondition(c, prefix, args));
    // Simply concatenate the where conditions
    if (prefix === '.where') {
      return parts.join('');
    }
    args.add('and');
    return 'and(' + parts.join(', ') + ')';
  }

  args = new Set<string>();

  // Handle nested conditions with a callback for OR conditions and nested ANDs/ORs
  const conditionsCode = conditions
    .map(c => transformCondition(c, 'cmp', args))
    .join(', ');

  args.add('cmp');
  args.add(type);
  const argsCode = [...args].sort().join(', ');

  return `.where(({${argsCode}}) => ${type}(${conditionsCode}))`;
}

function transformExistsCondition(
  condition: CorrelatedSubqueryCondition,
  prefix: '.where' | 'cmp',
  args: Set<string>,
): string {
  const {related, op} = condition;
  const relationship = extractRelationshipName(related);

  const nextSubquery = getNextExistsSubquery(related);

  // Check if subquery has additional properties
  const hasSubQueryProps =
    nextSubquery.where ||
    (nextSubquery.related && nextSubquery.related.length > 0) ||
    nextSubquery.orderBy ||
    nextSubquery.limit;

  if (op === 'EXISTS') {
    const flipped = condition.flip ? ', {flip: true}' : '';
    if (!hasSubQueryProps) {
      if (prefix === '.where') {
        return `.whereExists('${relationship}'${flipped})`;
      }
      args.add('exists');
      return `exists('${relationship}'${flipped})`;
    }

    if (prefix === '.where') {
      return `.whereExists('${relationship}', q => q${astToZQL(nextSubquery)}${flipped})`;
    }
    prefix satisfies 'cmp';
    args.add('exists');
    return `exists('${relationship}', q => q${astToZQL(nextSubquery)}${flipped})`;
  }

  op satisfies 'NOT EXISTS';

  if (hasSubQueryProps) {
    if (prefix === '.where') {
      return `.where(({exists, not}) => not(exists('${relationship}', q => q${astToZQL(
        nextSubquery,
      )})))`;
    }
    prefix satisfies 'cmp';
    args.add('not');
    args.add('exists');
    return `not(exists('${relationship}', q => q${astToZQL(nextSubquery)}))`;
  }

  if (prefix === '.where') {
    return `.where(({exists, not}) => not(exists('${relationship}')))`;
  }
  args.add('not');
  args.add('exists');

  return `not(exists('${relationship}')))`;
}

// If the `exists` is applied against a junction edge, both hops will have the same alias and both hops will be exists conditions.
function getNextExistsSubquery(related: CorrelatedSubquery): AST {
  if (
    related.subquery.where?.type === 'correlatedSubquery' &&
    related.subquery.where.related.subquery.alias?.includes(
      SUBQ_PREFIX + 'zhidden_',
    )
  ) {
    return getNextExistsSubquery(related.subquery.where.related);
  }

  return related.subquery;
}

function extractRelationshipName(related: CorrelatedSubquery): string {
  const alias = must(related.subquery.alias);
  return alias.startsWith(SUBQ_PREFIX)
    ? alias.substring(SUBQ_PREFIX.length)
    : alias;
}

function transformRelated(related: CorrelatedSubquery): string {
  const {alias} = related.subquery;
  if (!alias) return '';

  const relationship = alias;
  let code = `.related('${relationship}'`;

  // If the subquery has additional filters or configurations
  if (
    related.subquery.where ||
    (related.subquery.related && related.subquery.related.length > 0) ||
    related.subquery.orderBy ||
    related.subquery.limit
  ) {
    code += ', q => q' + astToZQL(related.subquery);
  }

  code += ')';
  return code;
}

function transformOrder(orderBy: Ordering): string {
  let code = '';
  for (const [field, direction] of orderBy) {
    code += `.orderBy('${field}', '${direction}')`;
  }
  return code;
}

function transformValuePosition(value: ValuePosition): string {
  switch (value.type) {
    case 'literal':
      return transformLiteral(value);
    case 'column':
      return `'${value.name}'`;
    case 'static':
      return transformParameter(value);
    default:
      unreachable(value);
  }
}

function transformLiteral(literal: LiteralReference): string {
  if (literal.value === null) {
    return 'null';
  }
  if (Array.isArray(literal.value)) {
    return JSON.stringify(literal.value);
  }
  if (typeof literal.value === 'string') {
    return `'${literal.value.replace(/'/g, "\\'")}'`;
  }
  return String(literal.value);
}

function transformParameter(param: Parameter): string {
  const fieldStr = Array.isArray(param.field)
    ? `[${param.field.map(f => `'${f}'`).join(', ')}]`
    : `'${param.field}'`;

  return `authParam(${fieldStr})`;
}
