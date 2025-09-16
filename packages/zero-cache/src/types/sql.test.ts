/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {describe, expect, test} from 'vitest';
import {id, idList} from './sql.ts';

describe('types/sql', () => {
  type Case = {
    id: string;
    escaped: string;
  };

  const cases: Case[] = [
    {
      id: 'simple',
      escaped: '"simple"',
    },
    {
      id: 'containing"quotes',
      escaped: '"containing""quotes"',
    },
    {
      id: 'name.with.dots',
      escaped: '"name.with.dots"',
    },
  ];

  for (const c of cases) {
    test(c.id, () => {
      expect(id(c.id)).toBe(c.escaped);
    });
  }

  type ListCase = {
    ids: string[];
    escaped: string;
  };

  const listCases: ListCase[] = [
    {
      ids: ['simple', 'containing"quotes', 'name.with.dots'],
      escaped: '"simple","containing""quotes","name.with.dots"',
    },
    {
      ids: ['singleton'],
      escaped: '"singleton"',
    },
  ];

  for (const c of listCases) {
    test(c.ids.join(','), () => {
      expect(idList(c.ids)).toBe(c.escaped);
    });
  }
});
