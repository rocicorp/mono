import {describe, expect, test} from 'vitest';
import {id, idList, lit} from './sql.ts';

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

  type LitCase = {
    value: string;
    escaped: string;
  };

  const litCases: LitCase[] = [
    {
      value: 'simple',
      escaped: "'simple'",
    },
    {
      value: "containing'quotes",
      escaped: "'containing''quotes'",
    },
    {
      value: "multiple'quotes'here",
      escaped: "'multiple''quotes''here'",
    },
    {
      value: '',
      escaped: "''",
    },
  ];

  for (const c of litCases) {
    test(`lit: ${c.value || '(empty)'}`, () => {
      expect(lit(c.value)).toBe(c.escaped);
    });
  }
});
