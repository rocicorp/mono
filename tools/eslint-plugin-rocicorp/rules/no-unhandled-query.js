/**
 * @fileoverview ESLint rule to detect unhandled Query instances
 * @author Rocicorp
 */

'use strict';

module.exports = {
  meta: {
    type: 'problem',
    docs: {
      description: 'Disallow unhandled Query instances',
      category: 'Best Practices',
      recommended: true,
      url: null,
    },
    fixable: null,
    schema: [],
    messages: {
      unhandledQuery: 'Query instance is not awaited, returned, or assigned to a variable. Query is immutable, so methods like .limit() have no effect unless the result is used.',
    },
  },

  create(context) {
    const services = context.parserServices;
    
    // Check if TypeScript services are available
    if (!services || !services.program || !services.esTreeNodeToTSNodeMap) {
      return {};
    }

    const checker = services.program.getTypeChecker();
    const tsNodeMap = services.esTreeNodeToTSNodeMap;

    /**
     * Check if a type is or extends the Query interface
     */
    function isQueryType(type) {
      if (!type) return false;
      
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
      const baseTypes = type.getBaseTypes && type.getBaseTypes();
      if (baseTypes && baseTypes.length > 0) {
        return baseTypes.some(baseType => isQueryType(baseType));
      }
      
      return false;
    }

    /**
     * Check if a node's return type is a Query
     */
    function returnsQuery(node) {
      const tsNode = tsNodeMap.get(node);
      if (!tsNode) return false;
      
      const type = checker.getTypeAtLocation(tsNode);
      if (!type) return false;
      
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
    function isProperlyHandled(node) {
      const parent = node.parent;
      if (!parent) return false;

      // Check various parent types that indicate the value is being used
      switch (parent.type) {
        // Assigned to a variable
        case 'VariableDeclarator':
          return parent.init === node;
        
        // Assigned to a property
        case 'AssignmentExpression':
          return parent.right === node;
        
        // Returned from a function
        case 'ReturnStatement':
          return true;
        
        // Awaited
        case 'AwaitExpression':
          return true;
        
        // Used in an arrow function return
        case 'ArrowFunctionExpression':
          return parent.body === node;
        
        // Part of an array
        case 'ArrayExpression':
          return true;
        
        // Part of an object
        case 'Property':
          return parent.value === node;
        
        // Passed as an argument
        case 'CallExpression':
          return parent.arguments.includes(node);
        
        // Used in a conditional
        case 'ConditionalExpression':
          return parent.consequent === node || parent.alternate === node;
        
        // Chained with then/catch/finally
        case 'MemberExpression':
          if (parent.object === node) {
            const prop = parent.property;
            if (prop.type === 'Identifier') {
              // Check for promise-like methods
              if (['then', 'catch', 'finally', 'run', 'materialize', 'preload'].includes(prop.name)) {
                // Make sure the member expression itself is also handled
                return isProperlyHandled(parent.parent);
              }
            }
          }
          return false;
        
        // Used in a sequence expression
        case 'SequenceExpression':
          // Only the last expression in a sequence is properly handled
          const expressions = parent.expressions;
          return expressions[expressions.length - 1] === node && isProperlyHandled(parent);
        
        // Yielded
        case 'YieldExpression':
          return true;
        
        // Exported
        case 'ExportDefaultDeclaration':
        case 'ExportNamedDeclaration':
          return true;
        
        // Check if parent is an expression statement (unhandled)
        case 'ExpressionStatement':
          return false;
        
        // For other member expressions that lead to method chains
        default:
          // If this is part of a chain, check if the chain result is handled
          if (parent.type === 'CallExpression' && parent.callee === node) {
            return false; // This means it's being called, not good enough
          }
          return false;
      }
    }

    return {
      CallExpression(node) {
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
      
      MemberExpression(node) {
        // Handle property access that might return a Query (like z.query.users)
        if (!returnsQuery(node)) {
          return;
        }

        // Only check if this is the end of a chain (not followed by more property access or calls)
        const parent = node.parent;
        if (parent && parent.type === 'MemberExpression' && parent.object === node) {
          return; // This is part of a longer chain
        }
        if (parent && parent.type === 'CallExpression' && parent.callee === node) {
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
};