/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {describe, expect, test} from 'vitest';
import {liteTableName} from './names.ts';

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
