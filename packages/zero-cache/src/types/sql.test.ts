import {describe, expect, test} from 'vitest';
import {id, idList} from './sql.js';

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
