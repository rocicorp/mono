import type {
  AST,
  Condition,
  CorrelatedSubqueryCondition,
} from '../../../zero-protocol/src/ast.ts';

export interface TransformResult {
  ast: AST;
  pathToOriginalRoot: string[];
}

/**
 * Transforms an AST with flip: true EXISTS conditions into an equivalent AST
 * without flip by restructuring the query.
 * 
 * When a flip is encountered:
 * 1. The subquery becomes the new root
 * 2. The parent becomes a subquery with swapped correlations
 * 3. WHERE conditions move with their table
 * 4. Returns the path to the original root for later extraction
 * 
 * For AND with multiple flips: Only transforms the FIRST flipped EXISTS.
 * This gives query authors control over optimization by ordering conditions.
 * 
 * Handles nested flips recursively.
 */
export function transformFlippedExists(ast: AST, depth = 0): TransformResult {
  // Prevent infinite recursion
  if (depth > 10) {
    console.warn('Max recursion depth reached in transformFlippedExists');
    return { ast, pathToOriginalRoot: [] };
  }
  
  // Check if the WHERE clause has any flipped EXISTS
  const flippedCondition = findFlippedExists(ast.where);
  
  if (!flippedCondition) {
    // No flipped EXISTS, return as-is
    return { ast, pathToOriginalRoot: [] };
  }
  
  // Found a flipped EXISTS, perform the transformation
  return transformAtCondition(ast, flippedCondition);
}

/**
 * Find the first flipped EXISTS condition in the WHERE clause.
 * We process depth-first to handle nested cases.
 */
function findFlippedExists(
  condition: Condition | undefined,
  visited: WeakSet<object> = new WeakSet(),
): CorrelatedSubqueryCondition | null {
  if (!condition) return null;
  
  // Prevent infinite recursion
  if (visited.has(condition)) return null;
  visited.add(condition);
  
  if (condition.type === 'correlatedSubquery') {
    // First check if this subquery's WHERE has a flipped EXISTS (nested case)
    const nestedFlip = findFlippedExists(condition.related.subquery.where, visited);
    if (nestedFlip) {
      return nestedFlip;
    }
    
    // Then check if this EXISTS itself is flipped
    if (condition.flip === true) {
      return condition;
    }
  }
  
  if (condition.type === 'and' || condition.type === 'or') {
    // For AND: Transform only the FIRST flipped EXISTS found
    // This gives query authors control over optimization by putting the best table first
    // For OR: Transform only the FIRST flipped EXISTS found (OR will need union/intersect in future)
    for (const subCondition of condition.conditions) {
      const found = findFlippedExists(subCondition, visited);
      if (found) return found;
    }
  }
  
  return null;
}

/**
 * Transform the AST at the location of the flipped condition.
 */
function transformAtCondition(
  ast: AST,
  flippedCondition: CorrelatedSubqueryCondition,
): TransformResult {
  const { related } = flippedCondition;
  const subquery = related.subquery;
  
  // Find the immediate parent of the flipped condition
  const parentInfo = findParentOfCondition(ast, flippedCondition);
  const parentAST = parentInfo || ast;
  
  // Check if there are multiple flipped EXISTS at the parent level
  const hasMultipleFlippedExists = parentAST.where?.type === 'and' && 
    hasMultipleFlippedExistsConditions(parentAST.where);
  
  if (hasMultipleFlippedExists) {
    // New approach: Split flipped vs non-flipped conditions
    const { flippedConditions, nonFlippedConditions } = splitFlippedConditions(parentAST.where, flippedCondition);
    
    // Convert remaining flipped conditions to regular EXISTS (for root level)
    const convertedFlippedConditions = flippedConditions.map(c => convertFlipToRegularExists(c));
    
    // Create the parent EXISTS condition (keeps non-flipped conditions)
    const parentExists = createParentExistsCondition(
      { ...parentAST, where: nonFlippedConditions },
      flippedCondition,
      ast,
    );
    
    // Combine at root level: subquery WHERE + parent EXISTS + other flipped EXISTS
    let rootWhere: Condition | undefined = combineConditions(subquery.where, parentExists);
    for (const convertedCondition of convertedFlippedConditions) {
      rootWhere = combineConditions(rootWhere, convertedCondition);
    }
    
    const newRootAST: AST = {
      ...subquery,
      where: rootWhere,
    };
    
    return {
      ast: newRootAST,
      pathToOriginalRoot: [parentAST.table],
    };
  } else {
    // Original approach: Keep conditions with the parent
    const parentWhereWithoutFlip = removeCondition(parentAST.where, flippedCondition);
    
    const parentExists = createParentExistsCondition(
      { ...parentAST, where: parentWhereWithoutFlip },
      flippedCondition,
      ast,
    );
    
    const newRootAST: AST = {
      ...subquery,
      where: combineConditions(subquery.where, parentExists),
    };
    
    return {
      ast: newRootAST,
      pathToOriginalRoot: [parentAST.table],
    };
  }
}

/**
 * Find the AST that directly contains the given condition in its WHERE clause.
 */
function findParentOfCondition(ast: AST, targetCondition: CorrelatedSubqueryCondition): AST | null {
  // First, check in WHERE clause subqueries to find the immediate parent
  const checkInWhere = (cond: Condition | undefined, currentAST: AST): AST | null => {
    if (!cond) return null;
    
    if (cond.type === 'correlatedSubquery' && cond !== targetCondition) {
      // Check if this subquery's WHERE contains our target condition
      if (containsCondition(cond.related.subquery.where, targetCondition)) {
        return cond.related.subquery;
      }
      // Otherwise recurse into the subquery
      const found = findParentOfCondition(cond.related.subquery, targetCondition);
      if (found) return found;
    }
    
    if (cond.type === 'and' || cond.type === 'or') {
      for (const sub of cond.conditions) {
        const found = checkInWhere(sub, currentAST);
        if (found) return found;
      }
    }
    
    return null;
  };
  
  // Check in WHERE clause subqueries first (for immediate parent)
  const foundInWhere = checkInWhere(ast.where, ast);
  if (foundInWhere) return foundInWhere;
  
  // Then check if this AST's WHERE directly contains the condition
  if (containsCondition(ast.where, targetCondition)) {
    return ast;
  }
  
  // Finally check in related subqueries
  if (ast.related) {
    for (const rel of ast.related) {
      const found = findParentOfCondition(rel.subquery, targetCondition);
      if (found) return found;
    }
  }
  
  return null;
}

/**
 * Check if a WHERE clause contains a specific condition.
 */
function containsCondition(where: Condition | undefined, target: CorrelatedSubqueryCondition): boolean {
  if (!where) return false;
  if (where === target) return true;
  
  if (where.type === 'and' || where.type === 'or') {
    return where.conditions.some(c => c === target);
  }
  
  return false;
}

/**
 * Create an EXISTS condition that checks for the parent table.
 * This replaces the flipped EXISTS in the restructured query.
 * The parentAST should already have the flipped condition removed.
 */
function createParentExistsCondition(
  parentAST: AST,
  flippedCondition: CorrelatedSubqueryCondition,
  originalRoot: AST,
): CorrelatedSubqueryCondition {
  const { related } = flippedCondition;
  
  // If parent is not the original root, we need to preserve the hierarchy
  let parentSubquery = parentAST;
  
  if (parentAST !== originalRoot) {
    // Find how the parent relates to the original root and preserve that relationship
    const parentToRootExists = createHierarchyToRoot(parentAST, originalRoot);
    if (parentToRootExists) {
      parentSubquery = {
        ...parentAST,
        where: combineConditions(parentAST.where, parentToRootExists),
      };
    }
  }
  
  return {
    type: 'correlatedSubquery',
    op: flippedCondition.op, // Preserve EXISTS or NOT EXISTS
    related: {
      system: related.system || 'client',
      subquery: {
        ...parentSubquery,
        // The parent becomes a subquery, so it needs an alias
        alias: parentSubquery.table,
      },
      correlation: {
        // Swap the correlation fields
        parentField: related.correlation.childField,
        childField: related.correlation.parentField,
      },
    },
    // No flip on the transformed condition
    flip: false,
  };
}

/**
 * Create an EXISTS relationship from parent back to the original root.
 * This preserves the hierarchy when we flip nested EXISTS conditions.
 */
function createHierarchyToRoot(parentAST: AST, originalRoot: AST): CorrelatedSubqueryCondition | null {
  // Find the correlation between original root and parent
  const rootCondition = findCorrelatedSubqueryCondition(originalRoot.where, parentAST.table);
  if (!rootCondition) return null;
  
  // Create the reverse EXISTS: parent -> root  
  return {
    type: 'correlatedSubquery',
    op: rootCondition.op, // Preserve EXISTS or NOT EXISTS
    related: {
      system: rootCondition.related.system || 'client',
      subquery: {
        ...originalRoot,
        where: removeCondition(originalRoot.where, rootCondition), // Remove the original parent relationship
        alias: originalRoot.table,
      },
      correlation: {
        // Swap the correlation fields
        parentField: rootCondition.related.correlation.childField,
        childField: rootCondition.related.correlation.parentField,
      },
    },
    flip: false,
  };
}

/**
 * Find a correlated subquery condition that points to a specific table.
 */
function findCorrelatedSubqueryCondition(
  condition: Condition | undefined,
  targetTable: string,
): CorrelatedSubqueryCondition | null {
  if (!condition) return null;
  
  if (condition.type === 'correlatedSubquery') {
    if (condition.related.subquery.table === targetTable) {
      return condition;
    }
  }
  
  if (condition.type === 'and' || condition.type === 'or') {
    for (const subCondition of condition.conditions) {
      const found = findCorrelatedSubqueryCondition(subCondition, targetTable);
      if (found) return found;
    }
  }
  
  return null;
}

/**
 * Check if an AND condition has multiple flipped EXISTS conditions.
 */
function hasMultipleFlippedExistsConditions(condition: Condition): boolean {
  if (condition.type !== 'and') return false;
  
  const flippedCount = condition.conditions.filter(c => 
    c.type === 'correlatedSubquery' && c.flip === true
  ).length;
  
  return flippedCount >= 2;
}

/**
 * Split conditions into flipped EXISTS vs non-flipped conditions.
 */
function splitFlippedConditions(
  where: Condition | undefined,
  excludeFlippedCondition: CorrelatedSubqueryCondition,
): { flippedConditions: CorrelatedSubqueryCondition[], nonFlippedConditions: Condition | undefined } {
  if (!where || where.type !== 'and') {
    return { flippedConditions: [], nonFlippedConditions: where };
  }
  
  const flippedConditions: CorrelatedSubqueryCondition[] = [];
  const nonFlippedConditions: Condition[] = [];
  
  for (const condition of where.conditions) {
    if (condition === excludeFlippedCondition) {
      // Skip the flipped condition we're transforming
      continue;
    }
    
    if (condition.type === 'correlatedSubquery' && condition.flip === true) {
      flippedConditions.push(condition);
    } else {
      nonFlippedConditions.push(condition);
    }
  }
  
  let combinedNonFlipped: Condition | undefined;
  if (nonFlippedConditions.length === 0) {
    combinedNonFlipped = undefined;
  } else if (nonFlippedConditions.length === 1) {
    combinedNonFlipped = nonFlippedConditions[0];
  } else {
    combinedNonFlipped = { type: 'and', conditions: nonFlippedConditions };
  }
  
  return { flippedConditions, nonFlippedConditions: combinedNonFlipped };
}


/**
 * Convert a flipped EXISTS condition to a regular EXISTS condition.
 */
function convertFlipToRegularExists(condition: Condition): Condition {
  if (condition.type === 'correlatedSubquery' && condition.flip === true) {
    return {
      ...condition,
      flip: false, // Remove the flip
    };
  }
  
  if (condition.type === 'and' || condition.type === 'or') {
    return {
      ...condition,
      conditions: condition.conditions.map(convertFlipToRegularExists),
    };
  }
  
  return condition;
}

/**
 * Remove a specific condition from a WHERE clause.
 */
function removeCondition(
  where: Condition | undefined,
  toRemove: CorrelatedSubqueryCondition,
): Condition | undefined {
  if (!where) return undefined;
  
  if (where === toRemove) {
    return undefined;
  }
  
  if (where.type === 'and') {
    const filtered = where.conditions.filter(c => c !== toRemove);
    if (filtered.length === 0) return undefined;
    if (filtered.length === 1) return filtered[0];
    return { ...where, conditions: filtered };
  }
  
  if (where.type === 'or') {
    const filtered = where.conditions.filter(c => c !== toRemove);
    if (filtered.length === 0) return undefined;
    if (filtered.length === 1) return filtered[0];
    return { ...where, conditions: filtered };
  }
  
  // For simple conditions or other correlated subqueries, just return as-is
  return where;
}

/**
 * Combine two conditions with AND.
 */
function combineConditions(
  cond1: Condition | undefined,
  cond2: Condition | undefined,
): Condition | undefined {
  if (!cond1) return cond2;
  if (!cond2) return cond1;
  
  // If either is already an AND, merge them
  if (cond1.type === 'and' && cond2.type === 'and') {
    return {
      type: 'and',
      conditions: [...cond1.conditions, ...cond2.conditions],
    };
  }
  
  if (cond1.type === 'and') {
    return {
      type: 'and',
      conditions: [...cond1.conditions, cond2],
    };
  }
  
  if (cond2.type === 'and') {
    return {
      type: 'and',
      conditions: [cond1, ...cond2.conditions],
    };
  }
  
  // Neither is AND, create a new AND
  return {
    type: 'and',
    conditions: [cond1, cond2],
  };
}

