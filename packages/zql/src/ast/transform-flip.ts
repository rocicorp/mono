import {must} from '../../../shared/src/must.ts';
import type {
  AST,
  Condition,
  CorrelatedSubqueryCondition,
  CorrelatedSubquery,
  Ordering,
  Bound,
  Correlation,
} from '../../../zero-protocol/src/ast.js';
import type {SourceSchema} from '../ivm/schema.ts';

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
  remainingConditions?: Condition | undefined;
  isOriginalRoot: boolean;
};

/**
 * Transforms an AST where an EXISTS has root: true.
 * Makes the subquery of that EXISTS the new root of the query.
 */
export function transformFlippedExists(
  ast: AST,
  rootSchema: SourceSchema,
): TransformResult | null {
  // Find the EXISTS with root: true and build the path to it
  const rootInfo = findRootMarkedExists(ast, [], ast);
  
  if (!rootInfo) {
    return null;
  }
  
  const {chain} = rootInfo;
  
  if (chain.length <= 1) {
    // No transformation needed
    return null;
  }
  
  // Rebuild the AST from the marked root
  const transformedAst = rebuildFromChain(chain, rootSchema);
  
  // Extract properties from the original root
  const originalRoot = chain.find(node => node.isOriginalRoot);
  const extractedProperties: ExtractedRootProperties = originalRoot
    ? {
        orderBy: originalRoot.ast.orderBy,
        start: originalRoot.ast.start,
        limit: originalRoot.ast.limit,
        related: originalRoot.ast.related,
      }
    : {};
  
  // Build the path from the new root to the original root
  const pathToRoot = buildPathToRoot(chain);
  
  return {
    transformedAst,
    extractedProperties,
    pathToRoot,
  };
}

/**
 * Finds an EXISTS condition with root: true and builds the chain to it.
 * Returns the chain from the current root to the marked root.
 */
function findRootMarkedExists(
  current: AST, 
  chain: ChainNode[],
  originalRoot: AST,
): {chain: ChainNode[]} | null {
  if (!current.where) {
    return null;
  }
  
  // Search for root: true in the current level
  const result = searchForRootInCondition(
    current.where, 
    current, 
    chain,
    chain.length === 0,
    originalRoot,
  );
  
  if (result) {
    return result;
  }
  
  return null;
}

/**
 * Recursively searches for a condition with root: true and builds chain.
 */
function searchForRootInCondition(
  condition: Condition,
  parentAST: AST,
  currentChain: ChainNode[],
  isOriginalRoot: boolean,
  originalRoot: AST,
): {chain: ChainNode[]} | null {
  if (condition.type === 'correlatedSubquery') {
    const nodeAlias = parentAST.alias || parentAST.table;
    
    if (condition.root === true) {
      // Found it! Build the final chain
      const chain = [...currentChain];
      
      // Add the parent node
      chain.push({
        ast: parentAST,
        alias: nodeAlias,
        correlation: condition.related.correlation,
        remainingConditions: removeCondition(parentAST.where, condition),
        isOriginalRoot,
      });
      
      // Add the target node (the one that should become root)
      chain.push({
        ast: condition.related.subquery,
        alias: condition.related.subquery.alias || condition.related.subquery.table,
        isOriginalRoot: false,
      });
      
      return {chain};
    }
    
    // Not marked as root, but search deeper
    if (condition.related.subquery.where) {
      // Build chain so far
      const newChain = [...currentChain];
      newChain.push({
        ast: parentAST,
        alias: nodeAlias,
        correlation: condition.related.correlation,
        remainingConditions: removeCondition(parentAST.where, condition),
        isOriginalRoot,
      });
      
      // Recursively search in the subquery
      const result = searchForRootInCondition(
        condition.related.subquery.where,
        condition.related.subquery,
        newChain,
        false,
        originalRoot,
      );
      
      if (result) {
        return result;
      }
    }
  } else if (condition.type === 'and') {
    // Search all branches for AND conditions only
    for (const child of condition.conditions) {
      const result = searchForRootInCondition(
        child,
        parentAST,
        currentChain,
        isOriginalRoot,
        originalRoot,
      );
      if (result) {
        return result;
      }
    }
  } else if (condition.type === 'or') {
    // Don't support root: true inside OR as it changes semantics
    return null;
  }
  
  return null;
}


/**
 * Rebuilds the AST from the chain, making the deepest node the new root.
 */
function rebuildFromChain(chain: ChainNode[], rootSchema: SourceSchema): AST {
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
    const filteredOrderBy = node.isOriginalRoot && node.ast.orderBy
      ? node.ast.orderBy.filter(([field]) => rootSchema.primaryKey.includes(field))
      : node.ast.orderBy;
    
    const subquery: ASTWithRootMarker = {
      table: node.ast.table,
      alias: node.alias,
      where: node.remainingConditions,
      ...(node.isOriginalRoot ? { wasRoot: true } : {}),
      ...(filteredOrderBy && filteredOrderBy.length > 0 ? { orderBy: filteredOrderBy } : {}),
    };

    // Invert the correlation (we're flipping the relationship)
    const invertedCorrelation = {
      parentField: must(node.correlation).childField,
      childField: must(node.correlation).parentField,
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
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        currentSubquery.where = combineConditions(
          currentSubquery.where,
          existsCondition,
        );
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
