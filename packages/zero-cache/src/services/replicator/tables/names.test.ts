import {describe, expect, test} from 'vitest';
import {liteTableName} from './names.js';

describe('tables/names', () => {
  (
    [
      {
        name: 'public schema',
        pg: {schema: 'public', name: 'issues'},
        lite: 'issues',
      },
      {
        name: 'zero schema',
        pg: {schema: 'zero', name: 'clients'},
        lite: 'zero.clients',
      },
    ] satisfies {
      name: string;
      pg: {schema: string; name: string};
      lite: string;
    }[]
  ).forEach(c => {
    test(c.name, () => {
      expect(liteTableName(c.pg)).toBe(c.lite);
    });
  });
});
