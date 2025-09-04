import {expect, test, describe} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.js';
import {
  transformFlippedExists,
  findPathToRoot,
  type ASTWithRootMarker,
} from './transform-flip.js';
import type {SourceSchema} from '../ivm/schema.js';

const mockRootSchema: SourceSchema = {
  tableName: 'users',
  columns: {
    id: {type: 'string'},
    name: {type: 'string'},
  },
  primaryKey: ['id'],
};

describe('transformFlippedExists', () => {
  test('transforms single nested flip (2 levels)', () => {
    // users WHERE EXISTS(foo WHERE EXISTS(bar)) - bar should become root
    const input: AST = {
      table: 'users',
      orderBy: [['name', 'asc']],
      limit: 10,
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'foo',
            where: {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              root: true,  // Only this one has root: true
              related: {
                subquery: {
                  table: 'bar',
                },
                correlation: {
                  parentField: ['fooId'],
                  childField: ['id'],
                },
                system: 'client',
              },
            },
          },
          correlation: {
            parentField: ['id'],
            childField: ['userId'],
          },
          system: 'client',
        },
      },
    };

    // Expected: bar WHERE EXISTS(foo WHERE EXISTS(users))
    const expectedAst: AST = {
      table: 'bar',
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'foo',
            alias: 'foo',
            where: {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              related: {
                subquery: {
                  table: 'users',
                  alias: 'users',
                  wasRoot: true,
                  orderBy: undefined,
                } as ASTWithRootMarker,
                correlation: {
                  parentField: ['userId'],  // Inverted from original
                  childField: ['id'],
                },
                system: 'client',
              },
            },
          },
          correlation: {
            parentField: ['id'],  // Inverted from original
            childField: ['fooId'],
          },
          system: 'client',
        },
      },
    };

    const result = transformFlippedExists(input, mockRootSchema);
    expect(result).not.toBeNull();
    expect(result!.transformedAst).toEqual(expectedAst);
    expect(result!.extractedProperties).toEqual({
      orderBy: [['name', 'asc']],
      limit: 10,
    });
    expect(result!.pathToRoot).toEqual(['foo', 'users']);
  });

  test('transforms 3-level nested flips', () => {
    // users -> foo -> bar -> baz - baz becomes root
    const input: AST = {
      table: 'users',
      orderBy: [['id', 'desc']],
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'foo',
            where: {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              related: {
                subquery: {
                  table: 'bar',
                  where: {
                    type: 'correlatedSubquery',
                    op: 'EXISTS',
                    root: true,  // Only this one has root: true
                    related: {
                      subquery: {
                        table: 'baz',
                      },
                      correlation: {
                        parentField: ['barId'],
                        childField: ['id'],
                      },
                      system: 'client',
                    },
                  },
                },
                correlation: {
                  parentField: ['fooId'],
                  childField: ['barId'],
                },
                system: 'client',
              },
            },
          },
          correlation: {
            parentField: ['id'],
            childField: ['userId'],
          },
          system: 'client',
        },
      },
    };

    // Expected: baz -> bar -> foo -> users
    const result = transformFlippedExists(input, mockRootSchema);
    expect(result).not.toBeNull();
    
    // The new root should be baz
    expect(result!.transformedAst.table).toBe('baz');
    
    // Path should go through all intermediate tables to reach the original root
    expect(result!.pathToRoot).toEqual(['bar', 'foo', 'users']);
    
    // Original root properties should be preserved
    expect(result!.extractedProperties.orderBy).toEqual([['id', 'desc']]);
  });

  test('handles partial flips (some levels not flipped)', () => {
    // users WHERE EXISTS(foo WHERE EXISTS(bar)) - foo becomes root
    const input: AST = {
      table: 'users',
      limit: 20,
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        root: true,  // Only this one has root: true
        related: {
          subquery: {
            table: 'foo',
            where: {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              // No root: true here
              related: {
                subquery: {
                  table: 'bar',
                },
                correlation: {
                  parentField: ['fooId'],
                  childField: ['id'],
                },
                system: 'client',
              },
            },
          },
          correlation: {
            parentField: ['id'],
            childField: ['userId'],
          },
          system: 'client',
        },
      },
    };

    // Expected: foo WHERE EXISTS(users) AND EXISTS(bar)
    const expectedAst: AST = {
      table: 'foo',
      alias: undefined,
      where: {
        type: 'and',
        conditions: [
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            related: {
              subquery: {
                table: 'bar',
              },
              correlation: {
                parentField: ['fooId'],
                childField: ['id'],
              },
              system: 'client',
            },
          },
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            related: {
              subquery: {
                table: 'users',
                alias: 'users',
                wasRoot: true,
                where: undefined,
              } as ASTWithRootMarker,
              correlation: {
                parentField: ['userId'],
                childField: ['id'],
              },
              system: 'client',
            },
          },
        ],
      },
    };

    const result = transformFlippedExists(input, mockRootSchema);
    expect(result).not.toBeNull();
    expect(result!.transformedAst).toEqual(expectedAst);
    expect(result!.extractedProperties.limit).toBe(20);
    expect(result!.pathToRoot).toEqual(['users']);
  });

  test('preserves WHERE conditions at each level', () => {
    const input: AST = {
      table: 'users',
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            left: {type: 'column', name: 'active'},
            op: '=',
            right: {type: 'literal', value: true},
          },
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            related: {
              subquery: {
                table: 'orders',
                where: {
                  type: 'and',
                  conditions: [
                    {
                      type: 'simple',
                      left: {type: 'column', name: 'status'},
                      op: '=',
                      right: {type: 'literal', value: 'completed'},
                    },
                    {
                      type: 'correlatedSubquery',
                      op: 'EXISTS',
                      root: true,  // Only this one has root: true
                      related: {
                        subquery: {
                          table: 'items',
                          where: {
                            type: 'simple',
                            left: {type: 'column', name: 'quantity'},
                            op: '>',
                            right: {type: 'literal', value: 0},
                          },
                        },
                        correlation: {
                          parentField: ['orderId'],
                          childField: ['id'],
                        },
                        system: 'client',
                      },
                    },
                  ],
                },
              },
              correlation: {
                parentField: ['id'],
                childField: ['userId'],
              },
              system: 'client',
            },
          },
        ],
      },
    };

    const result = transformFlippedExists(input, mockRootSchema);
    expect(result).not.toBeNull();
    
    // New root should be items
    expect(result!.transformedAst.table).toBe('items');
    
    // Items should keep its WHERE condition
    const itemsWhere = result!.transformedAst.where;
    expect(itemsWhere).toBeTruthy();
    
    if (itemsWhere?.type === 'and') {
      // Should have items' original condition + EXISTS(orders)
      expect(itemsWhere.conditions).toHaveLength(2);
      
      // Find the simple condition (quantity > 0)
      const simpleCondition = itemsWhere.conditions.find(c => c.type === 'simple');
      expect(simpleCondition).toBeTruthy();
      if (simpleCondition?.type === 'simple') {
        expect(simpleCondition.left).toEqual({type: 'column', name: 'quantity'});
        expect(simpleCondition.op).toBe('>');
        expect(simpleCondition.right).toEqual({type: 'literal', value: 0});
      }
    }
  });

  test('handles aliases correctly in nested flips', () => {
    const input: AST = {
      table: 'users',
      alias: 'u',  // Has existing alias
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'orders',
            alias: 'o',  // Has existing alias
            where: {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              root: true,  // Only this one has root: true
              related: {
                subquery: {
                  table: 'items',
                  // No alias
                },
                correlation: {
                  parentField: ['orderId'],
                  childField: ['id'],
                },
                system: 'client',
              },
            },
          },
          correlation: {
            parentField: ['id'],
            childField: ['userId'],
          },
          system: 'client',
        },
      },
    };

    const result = transformFlippedExists(input, mockRootSchema);
    expect(result).not.toBeNull();
    
    // Path should use existing aliases where available
    expect(result!.pathToRoot).toEqual(['o', 'u']);
  });

  test('returns null when no flips exist', () => {
    const input: AST = {
      table: 'users',
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        // No flip
        related: {
          subquery: {
            table: 'orders',
          },
          correlation: {
            parentField: ['id'],
            childField: ['userId'],
          },
          system: 'client',
        },
      },
    };

    const result = transformFlippedExists(input, mockRootSchema);
    expect(result).toBeNull();
  });

  test('does not support flips inside OR', () => {
    const input: AST = {
      table: 'users',
      where: {
        type: 'or',
        conditions: [
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            root: true,
            related: {
              subquery: {table: 'orders'},
              correlation: {
                parentField: ['id'],
                childField: ['userId'],
              },
              system: 'client',
            },
          },
          {
            type: 'simple',
            left: {type: 'column', name: 'active'},
            op: '=',
            right: {type: 'literal', value: true},
          },
        ],
      },
    };

    const result = transformFlippedExists(input, mockRootSchema);
    expect(result).toBeNull();
  });
});

describe('findPathToRoot in nested transforms', () => {
  test('finds path through multiple levels', () => {
    const ast: AST = {
      table: 'items',
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'orders',
            alias: 'orders',
            where: {
              type: 'correlatedSubquery',
              op: 'EXISTS',
              related: {
                subquery: {
                  table: 'users',
                  alias: 'users',
                  wasRoot: true,
                  orderBy: undefined,
                } as ASTWithRootMarker,
                correlation: {
                  parentField: ['userId'],
                  childField: ['id'],
                },
                system: 'client',
              },
            },
          },
          correlation: {
            parentField: ['id'],
            childField: ['orderId'],
          },
          system: 'client',
        },
      },
    };

    const path = findPathToRoot(ast);
    expect(path).not.toBeNull();
    expect(path).toEqual(['orders', 'users']);
  });
});