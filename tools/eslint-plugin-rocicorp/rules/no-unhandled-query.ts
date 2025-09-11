/**
 * @fileoverview ESLint rule to detect unhandled Query instances
 * @author Rocicorp
 */

import { ESLintUtils, TSESTree, AST_NODE_TYPES } from '@typescript-eslint/utils';
import type * as ts from 'typescript';

const createRule = ESLintUtils.RuleCreator(
  (name: string) => `https://github.com/rocicorp/mono/blob/main/tools/eslint-plugin-rocicorp/docs/${name}.md`
);

export const noUnhandledQuery = createRule({
  name: 'no-unhandled-query',
  meta: {
    type: 'problem',
    docs: {
      description: 'Disallow unhandled Query instances',
    },
    messages: {
      unhandledQuery: 'Query instance is not awaited, returned, or assigned to a variable. Query is immutable, so methods like .limit() have no effect unless the result is used.',
    },
    schema: [],
  },
  defaultOptions: [],

  create(context) {
    const services = ESLintUtils.getParserServices(context);
    const checker = services.program.getTypeChecker();

    /**
     * Check if a type is or extends the Query interface
     */
    function isQueryType(type: ts.Type): boolean {
      const typeString = checker.typeToString(type);
      
      // Check if it's directly a Query type
      if (typeString.includes('Query<')) {
        return true;
      }
      
      // Check the symbol name
      const symbol = type.getSymbol();
      if (symbol && symbol.getName() === 'Query') {
        return true;
      }
      
      // Check if it's a union type that includes Query
      if (type.isUnion()) {
        return type.types.some(t => isQueryType(t));
      }
      
      // Check base types
      const baseTypes = type.getBaseTypes?.();
      if (baseTypes && baseTypes.length > 0) {
        return baseTypes.some(baseType => isQueryType(baseType));
      }
      
      return false;
    }

    /**
     * Check if a node's return type is a Query
     */
    function returnsQuery(node: TSESTree.Node): boolean {
      const tsNode = services.esTreeNodeToTSNodeMap.get(node);
      const type = checker.getTypeAtLocation(tsNode);
      
      // For call expressions, get the return type
      const signatures = type.getCallSignatures();
      if (signatures && signatures.length > 0) {
        const returnType = signatures[0].getReturnType();
        return isQueryType(returnType);
      }
      
      return isQueryType(type);
    }

    /**
     * Check if a call expression result is properly handled
     */
    function isProperlyHandled(node: TSESTree.Node): boolean {
      const parent = node.parent;
      if (!parent) return false;

      switch (parent.type) {
        // Assigned to a variable
        case AST_NODE_TYPES.VariableDeclarator:
          return parent.init === node;
        
        // Assigned to a property
        case AST_NODE_TYPES.AssignmentExpression:
          return parent.right === node;
        
        // Returned from a function
        case AST_NODE_TYPES.ReturnStatement:
          return true;
        
        // Awaited
        case AST_NODE_TYPES.AwaitExpression:
          return true;
        
        // Used in an arrow function return
        case AST_NODE_TYPES.ArrowFunctionExpression:
          return parent.body === node;
        
        // Part of an array
        case AST_NODE_TYPES.ArrayExpression:
          return true;
        
        // Part of an object
        case AST_NODE_TYPES.Property:
          return parent.value === node;
        
        // Passed as an argument
        case AST_NODE_TYPES.CallExpression:
          return parent.arguments.includes(node as TSESTree.Expression);
        
        // Used in a conditional
        case AST_NODE_TYPES.ConditionalExpression:
          return parent.consequent === node || parent.alternate === node;
        
        // Chained with then/catch/finally or Query-specific methods
        case AST_NODE_TYPES.MemberExpression:
          if (parent.object === node) {
            const prop = parent.property;
            if (prop.type === AST_NODE_TYPES.Identifier) {
              // Check for promise-like methods and Query-specific methods
              if (['then', 'catch', 'finally', 'run', 'materialize', 'preload'].includes(prop.name)) {
                // Make sure the member expression itself is also handled
                return isProperlyHandled(parent.parent as TSESTree.Node);
              }
            }
          }
          return false;
        
        // Used in a sequence expression
        case AST_NODE_TYPES.SequenceExpression:
          // Only the last expression in a sequence is properly handled
          const expressions = parent.expressions;
          return expressions[expressions.length - 1] === node && isProperlyHandled(parent);
        
        // Yielded
        case AST_NODE_TYPES.YieldExpression:
          return true;
        
        // Exported
        case AST_NODE_TYPES.ExportDefaultDeclaration:
        case AST_NODE_TYPES.ExportNamedDeclaration:
          return true;
        
        // Check if parent is an expression statement (unhandled)
        case AST_NODE_TYPES.ExpressionStatement:
          return false;
        
        // For other member expressions that lead to method chains
        default:
          // If this is part of a chain, check if the chain result is handled
          if ('callee' in parent && parent.callee === node) {
            return false; // This means it's being called, not good enough
          }
          return false;
      }
    }

    return {
      CallExpression(node: TSESTree.CallExpression): void {
        // Check if this call returns a Query type
        if (!returnsQuery(node)) {
          return;
        }

        // Check if the Query is properly handled
        if (!isProperlyHandled(node)) {
          context.report({
            node,
            messageId: 'unhandledQuery',
          });
        }
      },
      
      MemberExpression(node: TSESTree.MemberExpression): void {
        // Handle property access that might return a Query (like z.query.users)
        if (!returnsQuery(node)) {
          return;
        }

        // Only check if this is the end of a chain (not followed by more property access or calls)
        const parent = node.parent;
        if (parent && parent.type === AST_NODE_TYPES.MemberExpression && parent.object === node) {
          return; // This is part of a longer chain
        }
        if (parent && parent.type === AST_NODE_TYPES.CallExpression && parent.callee === node) {
          return; // This is being called
        }

        // Check if the Query is properly handled
        if (!isProperlyHandled(node)) {
          context.report({
            node,
            messageId: 'unhandledQuery',
          });
        }
      },
    };
  },
});

export default noUnhandledQuery;