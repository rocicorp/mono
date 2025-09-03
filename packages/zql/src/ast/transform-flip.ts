import type {
  AST,
  Condition,
  CorrelatedSubqueryCondition,
  CorrelatedSubquery,
  Ordering,
  Bound,
} from '../../../zero-protocol/src/ast.js';

export type ExtractedRootProperties = {
  orderBy?: Ordering;
  start?: Bound;
  limit?: number;
  related?: readonly CorrelatedSubquery[];
};

export type TransformResult = {
  transformedAst: AST;
  extractedProperties: ExtractedRootProperties;
};

export type ASTWithRootMarker = AST & {
  wasRoot?: boolean;
};

/**
 * Transforms an AST with a flipped EXISTS condition.
 * Only supports flips at the top level or inside AND branches.
 * Flips inside OR branches are not supported as they change semantics.
 */
export function transformFlippedExists(ast: AST): TransformResult | null {
  const flippedCondition = findFlippedExistsCondition(ast.where);
  if (!flippedCondition) {
    return null;
  }

  const {subquery, correlation} = flippedCondition.related;
  
  // Strip presentation properties from root when moving to subquery position
  // Remove the flipped condition from the original WHERE clause
  // Keep other conditions to move down with the parent
  const remainingConditions = removeFlippedCondition(ast.where, flippedCondition);

  const rootAsSubquery: ASTWithRootMarker = {
    table: ast.table,
    alias: ast.alias,
    wasRoot: true,
    where: remainingConditions,
    // Strip presentation properties when moving to subquery
    // These don't have meaning in EXISTS context
  };

  // Invert the correlation for the flipped relationship
  const invertedCorrelation = {
    parentField: correlation.childField,
    childField: correlation.parentField,
  };

  // Create new EXISTS condition with inverted correlation
  const newExistsCondition: CorrelatedSubqueryCondition = {
    type: 'correlatedSubquery' as const,
    op: 'EXISTS',
    related: {
      ...flippedCondition.related,
      correlation: invertedCorrelation,
      subquery: rootAsSubquery,
    },
  };

  // Build the transformed AST with subquery as new root
  const transformedAst: AST = {
    ...subquery,
    where: combineConditions(subquery.where, newExistsCondition),
    orderBy: subquery.orderBy,
  };

  // Extract properties to re-apply after extraction
  const extractedProperties: ExtractedRootProperties = {
    orderBy: ast.orderBy,
    start: ast.start,
    limit: ast.limit,
    related: ast.related,
  };

  return {
    transformedAst,
    extractedProperties,
  };
}

/**
 * Finds a flipped EXISTS condition in the WHERE clause.
 * Only looks at top level or inside AND branches.
 * Returns null if flip is inside OR (unsupported).
 */
function findFlippedExistsCondition(
  condition: Condition | undefined,
  insideOr: boolean = false,
): CorrelatedSubqueryCondition | null {
  if (!condition) return null;

  if (
    condition.type === 'correlatedSubquery' &&
    condition.op === 'EXISTS' &&
    condition.flip === true
  ) {
    // Don't support flips inside OR branches
    if (insideOr) {
      return null;
    }
    return condition;
  }

  if (condition.type === 'and') {
    // AND branches maintain the same insideOr context
    for (const child of condition.conditions) {
      const found = findFlippedExistsCondition(child, insideOr);
      if (found) return found;
    }
  }

  if (condition.type === 'or') {
    // Mark that we're inside an OR - flips not supported here
    for (const child of condition.conditions) {
      const found = findFlippedExistsCondition(child, true);
      if (found) return found; // Will be null due to insideOr check
    }
  }

  return null;
}

/**
 * Removes the flipped condition from a WHERE clause tree.
 * Preserves all other conditions.
 */
function removeFlippedCondition(
  condition: Condition | undefined,
  toRemove: CorrelatedSubqueryCondition,
): Condition | undefined {
  if (!condition) return undefined;

  // Direct match - remove this condition
  if (condition === toRemove) {
    return undefined;
  }

  // AND node - recursively remove from children
  if (condition.type === 'and') {
    const filtered = condition.conditions
      .map(c => removeFlippedCondition(c, toRemove))
      .filter((c): c is Condition => c !== undefined);
    
    if (filtered.length === 0) return undefined;
    if (filtered.length === 1) return filtered[0];
    return {
      type: 'and',
      conditions: filtered,
    };
  }

  // OR node - recursively remove from children
  if (condition.type === 'or') {
    const filtered = condition.conditions
      .map(c => removeFlippedCondition(c, toRemove))
      .filter((c): c is Condition => c !== undefined);
    
    if (filtered.length === 0) return undefined;
    if (filtered.length === 1) return filtered[0];
    return {
      type: 'or',
      conditions: filtered,
    };
  }

  // Other condition types - keep as is
  return condition;
}

/**
 * Combines multiple conditions into a single condition.
 * Returns undefined if no conditions provided.
 * Returns single condition if only one provided.
 * Combines multiple with AND.
 */
function combineConditions(
  ...conditions: (Condition | undefined)[]
): Condition | undefined {
  const validConditions = conditions.filter((c): c is Condition => c !== undefined);
  
  if (validConditions.length === 0) return undefined;
  if (validConditions.length === 1) return validConditions[0];
  
  return {
    type: 'and',
    conditions: validConditions,
  };
}

/**
 * Traverses a transformed AST to find the node marked as the original root.
 * Uses BFS to explore the AST tree structure.
 */
export function findRootInTransformedAst(ast: AST): ASTWithRootMarker | null {
  const queue: AST[] = [ast];
  
  while (queue.length > 0) {
    const current = queue.shift()!;
    
    // Check if this node was the original root
    if ((current as ASTWithRootMarker).wasRoot) {
      return current as ASTWithRootMarker;
    }
    
    // Explore WHERE conditions for subqueries
    if (current.where) {
      const subqueries = extractSubqueriesFromCondition(current.where);
      queue.push(...subqueries);
    }
    
    // Explore related subqueries
    if (current.related) {
      for (const rel of current.related) {
        queue.push(rel.subquery);
      }
    }
  }
  
  return null;
}

/**
 * Extracts all subqueries from a condition tree.
 */
function extractSubqueriesFromCondition(condition: Condition): AST[] {
  const subqueries: AST[] = [];
  
  if (condition.type === 'correlatedSubquery') {
    subqueries.push(condition.related.subquery);
  } else if (condition.type === 'and' || condition.type === 'or') {
    for (const child of condition.conditions) {
      subqueries.push(...extractSubqueriesFromCondition(child));
    }
  }
  
  return subqueries;
}