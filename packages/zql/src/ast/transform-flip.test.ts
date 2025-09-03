import {expect, test, describe} from 'vitest';
import type {AST, Condition, CorrelatedSubqueryCondition} from '../../../zero-protocol/src/ast.js';
import {transformFlippedExists, findRootInTransformedAst, type ASTWithRootMarker} from './transform-flip.js';

describe('transformFlippedExists', () => {
  test('transforms simple flipped EXISTS', () => {
    const input: AST = {
      table: 'users',
      orderBy: [['name', 'asc']],
      limit: 10,
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        flip: true,
        related: {
          subquery: {
            table: 'orders',
            alias: 'o',
          },
          correlation: {
            parentField: ['id'],
            childField: ['userId'],
          },
          system: 'client',
        },
      } as CorrelatedSubqueryCondition,
    };

    const expectedAst: AST = {
      table: 'orders',
      alias: 'o',
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'users',
            wasRoot: true,
            // orderBy, limit removed when moved to subquery position
          } as ASTWithRootMarker,
          correlation: {
            parentField: ['userId'],  // Inverted
            childField: ['id'],       // Inverted
          },
          system: 'client',
        },
      },
    };

    const expectedExtractedProperties = {
      orderBy: [['name', 'asc']],
      limit: 10,
    };

    const result = transformFlippedExists(input);
    expect(result).not.toBeNull();
    expect(result!.transformedAst).toEqual(expectedAst);
    expect(result!.extractedProperties).toEqual(expectedExtractedProperties);
  });

  test('handles flipped EXISTS with additional WHERE conditions', () => {
    const input: AST = {
      table: 'users',
      where: {
        type: 'and',
        conditions: [
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            flip: true,
            related: {
              subquery: {table: 'orders'},
              correlation: {
                parentField: ['id'],
                childField: ['userId'],
              },
              system: 'client',
            },
          } as CorrelatedSubqueryCondition,
          {
            type: 'simple',
            left: {type: 'column', tableID: 'users', columnID: 'active'},
            op: '=',
            right: {type: 'literal', value: true, valueType: 'boolean'},
          },
        ],
      },
    };

    const expectedAst: AST = {
      table: 'orders',
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'users',
            wasRoot: true,
            where: {
              type: 'simple',
              left: {type: 'column', tableID: 'users', columnID: 'active'},
              op: '=',
              right: {type: 'literal', value: true, valueType: 'boolean'},
            },
          } as ASTWithRootMarker,
          correlation: {
            parentField: ['userId'],
            childField: ['id'],
          },
          system: 'client',
        },
      },
    };

    const result = transformFlippedExists(input);
    expect(result).not.toBeNull();
    expect(result!.transformedAst).toEqual(expectedAst);
    expect(result!.extractedProperties).toEqual({});
  });

  test('preserves subquery WHERE conditions', () => {
    const input: AST = {
      table: 'users',
      orderBy: [['id', 'asc']],
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        flip: true,
        related: {
          subquery: {
            table: 'orders',
            where: {
              type: 'simple',
              left: {type: 'column', tableID: 'orders', columnID: 'status'},
              op: '=',
              right: {type: 'literal', value: 'completed', valueType: 'string'},
            },
          },
          correlation: {
            parentField: ['id'],
            childField: ['userId'],
          },
          system: 'client',
        },
      } as CorrelatedSubqueryCondition,
    };

    const expectedAst: AST = {
      table: 'orders',
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            left: {type: 'column', tableID: 'orders', columnID: 'status'},
            op: '=',
            right: {type: 'literal', value: 'completed', valueType: 'string'},
          },
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            related: {
              subquery: {
                table: 'users',
                wasRoot: true,
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

    const result = transformFlippedExists(input);
    expect(result).not.toBeNull();
    expect(result!.transformedAst).toEqual(expectedAst);
    expect(result!.extractedProperties).toEqual({
      orderBy: [['id', 'asc']],
    });
  });

  test('returns null for non-flipped EXISTS', () => {
    const input: AST = {
      table: 'users',
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        // No flip: true
        related: {
          subquery: {table: 'orders'},
          correlation: {
            parentField: ['id'],
            childField: ['userId'],
          },
          system: 'client',
        },
      },
    };

    const result = transformFlippedExists(input);
    expect(result).toBeNull();
  });

  test('returns null when no WHERE clause', () => {
    const input: AST = {
      table: 'users',
    };

    const result = transformFlippedExists(input);
    expect(result).toBeNull();
  });

  test('does not support flipped EXISTS inside OR', () => {
    const input: AST = {
      table: 'users',
      where: {
        type: 'or',
        conditions: [
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            flip: true,
            related: {
              subquery: {table: 'orders'},
              correlation: {
                parentField: ['id'],
                childField: ['userId'],
              },
              system: 'client',
            },
          } as CorrelatedSubqueryCondition,
          {
            type: 'simple',
            left: {type: 'column', tableID: 'users', columnID: 'active'},
            op: '=',
            right: {type: 'literal', value: true, valueType: 'boolean'},
          },
        ],
      },
    };

    const result = transformFlippedExists(input);
    expect(result).toBeNull(); // Should not transform flips inside OR
  });

  test('preserves related subqueries in extracted properties', () => {
    const input: AST = {
      table: 'users',
      related: [
        {
          subquery: {table: 'posts'},
          correlation: {
            parentField: ['id'],
            childField: ['authorId'],
          },
          system: 'client',
        },
      ],
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        flip: true,
        related: {
          subquery: {table: 'orders'},
          correlation: {
            parentField: ['id'],
            childField: ['userId'],
          },
          system: 'client',
        },
      } as CorrelatedSubqueryCondition,
    };

    const expectedAst: AST = {
      table: 'orders',
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'users',
            wasRoot: true,
          } as ASTWithRootMarker,
          correlation: {
            parentField: ['userId'],
            childField: ['id'],
          },
          system: 'client',
        },
      },
    };

    const expectedExtractedProperties = {
      related: [
        {
          subquery: {table: 'posts'},
          correlation: {
            parentField: ['id'],
            childField: ['authorId'],
          },
          system: 'client',
        },
      ],
    };

    const result = transformFlippedExists(input);
    expect(result).not.toBeNull();
    expect(result!.transformedAst).toEqual(expectedAst);
    expect(result!.extractedProperties).toEqual(expectedExtractedProperties);
  });

  test('handles complex nested AND conditions', () => {
    const input: AST = {
      table: 'users',
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            left: {type: 'column', tableID: 'users', columnID: 'age'},
            op: '>',
            right: {type: 'literal', value: 18, valueType: 'number'},
          },
          {
            type: 'and',
            conditions: [
              {
                type: 'correlatedSubquery',
                op: 'EXISTS',
                flip: true,
                related: {
                  subquery: {table: 'orders'},
                  correlation: {
                    parentField: ['id'],
                    childField: ['userId'],
                  },
                  system: 'client',
                },
              } as CorrelatedSubqueryCondition,
              {
                type: 'simple',
                left: {type: 'column', tableID: 'users', columnID: 'verified'},
                op: '=',
                right: {type: 'literal', value: true, valueType: 'boolean'},
              },
            ],
          },
        ],
      },
    };

    const expectedAst: AST = {
      table: 'orders',
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'users',
            wasRoot: true,
            where: {
              type: 'and',
              conditions: [
                {
                  type: 'simple',
                  left: {type: 'column', tableID: 'users', columnID: 'age'},
                  op: '>',
                  right: {type: 'literal', value: 18, valueType: 'number'},
                },
                {
                  type: 'simple',
                  left: {type: 'column', tableID: 'users', columnID: 'verified'},
                  op: '=',
                  right: {type: 'literal', value: true, valueType: 'boolean'},
                },
              ],
            },
          } as ASTWithRootMarker,
          correlation: {
            parentField: ['userId'],
            childField: ['id'],
          },
          system: 'client',
        },
      },
    };

    const result = transformFlippedExists(input);
    expect(result).not.toBeNull();
    expect(result!.transformedAst).toEqual(expectedAst);
    expect(result!.extractedProperties).toEqual({});
  });

  test('handles pagination properties', () => {
    const input: AST = {
      table: 'users',
      start: {
        exclusive: false,
        row: {id: 10},
      },
      limit: 20,
      orderBy: [['created', 'desc']],
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        flip: true,
        related: {
          subquery: {
            table: 'orders',
            orderBy: [['price', 'asc']],
          },
          correlation: {
            parentField: ['id'],
            childField: ['userId'],
          },
          system: 'client',
        },
      } as CorrelatedSubqueryCondition,
    };

    const expectedAst: AST = {
      table: 'orders',
      orderBy: [['price', 'asc']], // Subquery's orderBy preserved at root level
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'users',
            wasRoot: true,
            // All presentation properties stripped
          } as ASTWithRootMarker,
          correlation: {
            parentField: ['userId'],
            childField: ['id'],
          },
          system: 'client',
        },
      },
    };

    const expectedExtractedProperties = {
      start: {
        exclusive: false,
        row: {id: 10},
      },
      limit: 20,
      orderBy: [['created', 'desc']],
    };

    const result = transformFlippedExists(input);
    expect(result).not.toBeNull();
    expect(result!.transformedAst).toEqual(expectedAst);
    expect(result!.extractedProperties).toEqual(expectedExtractedProperties);
  });
});

describe('findRootInTransformedAst', () => {
  test('finds root marked in simple transformed AST', () => {
    const ast: AST = {
      table: 'orders',
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'users',
            wasRoot: true,
          } as ASTWithRootMarker,
          correlation: {
            parentField: ['userId'],
            childField: ['id'],
          },
          system: 'client',
        },
      },
    };

    const root = findRootInTransformedAst(ast);
    expect(root).not.toBeNull();
    expect(root).toEqual({
      table: 'users',
      wasRoot: true,
    });
  });

  test('finds root in deeply nested structure', () => {
    const markedRoot: ASTWithRootMarker = {
      table: 'users',
      wasRoot: true,
    };

    const ast: AST = {
      table: 'products',
      where: {
        type: 'and',
        conditions: [
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            related: {
              subquery: {
                table: 'categories',
                where: {
                  type: 'correlatedSubquery',
                  op: 'EXISTS',
                  related: {
                    subquery: markedRoot,
                    correlation: {
                      parentField: ['id'],
                      childField: ['userId'],
                    },
                    system: 'client',
                  },
                },
              },
              correlation: {
                parentField: ['categoryId'],
                childField: ['id'],
              },
              system: 'client',
            },
          },
        ],
      },
    };

    const root = findRootInTransformedAst(ast);
    expect(root).not.toBeNull();
    expect(root).toEqual(markedRoot);
  });

  test('returns null when no root marked', () => {
    const ast: AST = {
      table: 'orders',
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'users',
            // No wasRoot marker
          },
          correlation: {
            parentField: ['userId'],
            childField: ['id'],
          },
          system: 'client',
        },
      },
    };

    const root = findRootInTransformedAst(ast);
    expect(root).toBeNull();
  });

  test('finds root in related subqueries', () => {
    const markedRoot: ASTWithRootMarker = {
      table: 'users',
      wasRoot: true,
    };

    const ast: AST = {
      table: 'orders',
      related: [
        {
          subquery: markedRoot,
          correlation: {
            parentField: ['userId'],
            childField: ['id'],
          },
          system: 'client',
        },
      ],
    };

    const root = findRootInTransformedAst(ast);
    expect(root).not.toBeNull();
    expect(root).toEqual(markedRoot);
  });
});