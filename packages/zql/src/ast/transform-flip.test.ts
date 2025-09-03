import {expect, test, describe} from 'vitest';
import type {AST} from '../../../zero-protocol/src/ast.js';
import {transformFlippedExists, findPathToRoot, type ASTWithRootMarker} from './transform-flip.js';

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
      },
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
            alias: 'users_flipped',  // Generated alias
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
    expect(result!.pathToRoot).toEqual(['users_flipped']);
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

    const expectedAst: AST = {
      table: 'orders',
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'users',
            alias: 'users_flipped',
            wasRoot: true,
            where: {
              type: 'simple',
              left: {type: 'column', name: 'active'},
              op: '=',
              right: {type: 'literal', value: true},
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
    expect(result!.pathToRoot).toEqual(['users_flipped']);
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
              left: {type: 'column', name: 'status'},
              op: '=',
              right: {type: 'literal', value: 'completed'},
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

    const expectedAst: AST = {
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
            related: {
              subquery: {
                table: 'users',
                alias: 'users_flipped',
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
    expect(result!.pathToRoot).toEqual(['users_flipped']);
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
            alias: 'users_flipped',
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
    expect(result!.pathToRoot).toEqual(['users_flipped']);
  });

  test('handles complex nested AND conditions', () => {
    const input: AST = {
      table: 'users',
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            left: {type: 'column', name: 'age'},
            op: '>',
            right: {type: 'literal', value: 18},
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
              },
              {
                type: 'simple',
                left: {type: 'column', name: 'verified'},
                op: '=',
                right: {type: 'literal', value: true},
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
            alias: 'users_flipped',
            wasRoot: true,
            where: {
              type: 'and',
              conditions: [
                {
                  type: 'simple',
                  left: {type: 'column', name: 'age'},
                  op: '>',
                  right: {type: 'literal', value: 18},
                },
                {
                  type: 'simple',
                  left: {type: 'column', name: 'verified'},
                  op: '=',
                  right: {type: 'literal', value: true},
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
    expect(result!.pathToRoot).toEqual(['users_flipped']);
  });

  test('preserves existing alias when flipping', () => {
    const input: AST = {
      table: 'users',
      alias: 'u',  // Has existing alias
      orderBy: [['name', 'asc']],
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        flip: true,
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

    const expectedAst: AST = {
      table: 'orders',
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'users',
            alias: 'u',  // Preserved existing alias
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

    const result = transformFlippedExists(input);
    expect(result).not.toBeNull();
    expect(result!.transformedAst).toEqual(expectedAst);
    expect(result!.pathToRoot).toEqual(['u']);  // Uses existing alias in path
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
      },
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
            alias: 'users_flipped',
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
    expect(result!.pathToRoot).toEqual(['users_flipped']);
  });
});

describe('findPathToRoot', () => {
  test('finds path to root in simple transformed AST', () => {
    const ast: AST = {
      table: 'orders',
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          subquery: {
            table: 'users',
            alias: 'users_flipped',
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

    const path = findPathToRoot(ast);
    expect(path).not.toBeNull();
    expect(path).toEqual(['users_flipped']);
  });

  test('finds path in deeply nested structure', () => {
    const markedRoot: ASTWithRootMarker = {
      table: 'users',
      alias: 'u',
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
                alias: 'cat',
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

    const path = findPathToRoot(ast);
    expect(path).not.toBeNull();
    expect(path).toEqual(['cat', 'u']);  // Path through categories to users
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
            alias: 'u',
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

    const path = findPathToRoot(ast);
    expect(path).toBeNull();
  });

  test('finds path through related subqueries', () => {
    const markedRoot: ASTWithRootMarker = {
      table: 'users',
      alias: 'users_rel',
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

    const path = findPathToRoot(ast);
    expect(path).not.toBeNull();
    expect(path).toEqual(['users_rel']);
  });
});