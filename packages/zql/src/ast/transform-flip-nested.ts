import type {
  AST,
  Condition,
  CorrelatedSubqueryCondition,
  CorrelatedSubquery,
  Ordering,
  Bound,
  Correlation,
} from '../../../zero-protocol/src/ast.js';

export type ExtractedRootProperties = {
  orderBy?: Ordering | undefined;
  start?: Bound | undefined;
  limit?: number | undefined;
  related?: readonly CorrelatedSubquery[] | undefined;
};

export type TransformResult = {
  transformedAst: AST;
  extractedProperties: ExtractedRootProperties;
  pathToRoot: string[];
};

export type ASTWithRootMarker = AST & {
  wasRoot?: boolean;
};

/**
 * Represents a table in the flip chain with its context
 */
type ChainNode = {
  ast: AST;
  alias: string;
  correlation?: Correlation;
  remainingConditions?: Condition;
  isOriginalRoot: boolean;
};

/**
 * Transforms an AST with nested flipped EXISTS conditions.
 * Handles any depth of nested flips by extracting the chain and rebuilding.
 */
export function transformNestedFlippedExists(ast: AST): TransformResult | null {
  // Extract the chain of flipped EXISTS
  const chain = extractFlipChain(ast);
  
  if (!chain || chain.length === 0) {
    return null;
  }
  
  // Rebuild the AST from the deepest node as the new root
  const transformedAst = rebuildFromChain(chain);
  
  // Extract properties from the original root
  const originalRoot = chain.find(node => node.isOriginalRoot);
  const extractedProperties: ExtractedRootProperties = originalRoot ? {
    orderBy: originalRoot.ast.orderBy,
    start: originalRoot.ast.start,
    limit: originalRoot.ast.limit,
    related: originalRoot.ast.related,
  } : {};
  
  // Build the path from the new root to the original root
  // This is the sequence of aliases to traverse to find the original root
  const pathToRoot = buildPathToRoot(chain);
  
  return {
    transformedAst,
    extractedProperties,
    pathToRoot,
  };
}

/**
 * Extracts the chain of tables involved in flipped EXISTS conditions.
 * Returns them in order from root to deepest.
 */
function extractFlipChain(ast: AST): ChainNode[] | null {
  const chain: ChainNode[] = [];
  let current = ast;
  let isFirst = true;
  
  while (current) {
    const flippedCondition = findFlippedExistsCondition(current.where);
    
    if (!flippedCondition) {
      // No more flips
      if (chain.length === 0) {
        // No flips at all
        return null;
      }
      // Add the last non-flipped node (it becomes the new root)
      chain.push({
        ast: current,
        alias: current.alias || `${current.table}_leaf`,
        isOriginalRoot: false,
      });
      break;
    }
    
    // Extract this level's info
    const remainingConditions = removeCondition(current.where, flippedCondition);
    const nodeAlias = current.alias || `${current.table}_flipped`;
    
    chain.push({
      ast: current,
      alias: nodeAlias,
      correlation: flippedCondition.related.correlation,
      remainingConditions,
      isOriginalRoot: isFirst,
    });
    
    // Move to the next level
    current = flippedCondition.related.subquery;
    isFirst = false;
  }
  
  return chain.length > 0 ? chain : null;
}

/**
 * Rebuilds the AST from the chain, making the deepest node the new root.
 */
function rebuildFromChain(chain: ChainNode[]): AST {
  if (chain.length === 0) {
    throw new Error('Cannot rebuild from empty chain');
  }
  
  // Start with the deepest node (last in chain) as the new root
  const newRoot = chain[chain.length - 1];
  let currentAst: AST = {
    ...newRoot.ast,
    alias: undefined, // Root doesn't need an alias at the top level
  };
  
  // Build nested EXISTS from the bottom up
  // We need to create a nested structure, not just combine conditions
  let currentSubquery: AST | null = null;
  
  // Work backwards through the chain (excluding the new root)
  for (let i = chain.length - 2; i >= 0; i--) {
    const node = chain[i];
    
    // Create the subquery for this level
    const subquery: ASTWithRootMarker = {
      table: node.ast.table,
      alias: node.alias,
      where: node.remainingConditions,
      ...(node.isOriginalRoot ? {wasRoot: true} : {}),
    };
    
    // Invert the correlation (we're flipping the relationship)
    const invertedCorrelation = node.correlation ? {
      parentField: node.correlation.childField,
      childField: node.correlation.parentField,
    } : {
      // Default correlation if not specified
      parentField: ['id'],
      childField: [`${node.ast.table}Id`],
    };
    
    // Create EXISTS condition pointing to this subquery
    const existsCondition: CorrelatedSubqueryCondition = {
      type: 'correlatedSubquery' as const,
      op: 'EXISTS',
      related: {
        correlation: invertedCorrelation,
        subquery,
        system: 'client',
      },
    };
    
    if (i === chain.length - 2) {
      // First iteration - add EXISTS to the new root
      currentAst = {
        ...currentAst,
        where: combineConditions(currentAst.where, existsCondition),
      };
      currentSubquery = subquery;
    } else {
      // Subsequent iterations - add EXISTS to the previous subquery
      if (currentSubquery) {
        currentSubquery.where = combineConditions(currentSubquery.where, existsCondition);
        currentSubquery = subquery;
      }
    }
  }
  
  return currentAst;
}

/**
 * Builds the path of aliases from the new root to the original root.
 */
function buildPathToRoot(chain: ChainNode[]): string[] {
  const path: string[] = [];
  
  // Find where the original root ended up
  // It should be in the nested EXISTS conditions
  // The path is the sequence of aliases to traverse
  for (let i = chain.length - 2; i >= 0; i--) {
    path.push(chain[i].alias);
    if (chain[i].isOriginalRoot) {
      break;
    }
  }
  
  return path;
}

/**
 * Finds a flipped EXISTS condition in the WHERE clause.
 * Only looks at top level or inside AND branches.
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
 * Removes a specific condition from a WHERE clause tree.
 */
function removeCondition(
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
      .map(c => removeCondition(c, toRemove))
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
      .map(c => removeCondition(c, toRemove))
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
 */
function combineConditions(
  ...conditions: (Condition | undefined)[]
): Condition | undefined {
  const validConditions = conditions.filter(
    (c): c is Condition => c !== undefined,
  );

  if (validConditions.length === 0) return undefined;
  if (validConditions.length === 1) return validConditions[0];

  return {
    type: 'and',
    conditions: validConditions,
  };
}

/**
 * Traverses a transformed AST to find the path to the node marked as the original root.
 * Returns an array of aliases representing the path to traverse.
 */
export function findPathToRoot(ast: AST): string[] | null {
  type QueueItem = {
    node: AST;
    path: string[];
  };

  const queue: QueueItem[] = [{node: ast, path: []}];

  while (queue.length > 0) {
    const {node: current, path} = queue.shift()!;

    // Check if this node was the original root
    if ((current as ASTWithRootMarker).wasRoot) {
      return path;
    }

    // Explore WHERE conditions for subqueries
    if (current.where) {
      const subqueriesWithAliases = extractSubqueriesWithAliasesFromCondition(
        current.where,
      );
      for (const {subquery, alias} of subqueriesWithAliases) {
        // Use alias if available, otherwise use table name
        const relationshipName = alias || subquery.alias || subquery.table;
        queue.push({
          node: subquery,
          path: [...path, relationshipName],
        });
      }
    }

    // Explore related subqueries
    if (current.related) {
      for (const rel of current.related) {
        // For related, use the subquery's alias
        const relationshipName = rel.subquery.alias || rel.subquery.table;
        queue.push({
          node: rel.subquery,
          path: [...path, relationshipName],
        });
      }
    }
  }

  return null;
}

/**
 * Extracts all subqueries with their relationship aliases from a condition tree.
 */
function extractSubqueriesWithAliasesFromCondition(
  condition: Condition,
): Array<{subquery: AST; alias: string | undefined}> {
  const results: Array<{subquery: AST; alias: string | undefined}> = [];

  if (condition.type === 'correlatedSubquery') {
    // The alias for a subquery in EXISTS is the subquery's own alias
    results.push({
      subquery: condition.related.subquery,
      alias: condition.related.subquery.alias,
    });
  } else if (condition.type === 'and' || condition.type === 'or') {
    for (const child of condition.conditions) {
      results.push(...extractSubqueriesWithAliasesFromCondition(child));
    }
  }

  return results;
}