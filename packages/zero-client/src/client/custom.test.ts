import {expectTypeOf, test} from 'vitest';

import {schema} from '../../../zql/src/query/test/test-schemas.ts';
import type {CustomMutatorDefs, MakeCustomMutatorInterfaces} from './custom.ts';
type Schema = typeof schema;

test('argument types are preserved on the generated mutator interface', () => {
  const mutators = {
    issue: {
      setTitle: (tx, id: string, title: string) => {
        tx.mutate.issue.update({id, title});
      },
      setProps: (
        tx,
        id: string,
        title: string,
        status: 'open' | 'closed',
        assignee: string,
      ) => {
        tx.mutate.issue.update({
          id,
          title,
          closed: status === 'closed',
          ownerId: assignee,
        });
      },
    },
    nonTableNamespace: {
      doThing: (_tx, _arg1: string, _arg2: number) => {
        throw new Error('not implemented');
      },
    },
  } satisfies CustomMutatorDefs<Schema>;

  type MutatorsInterface = MakeCustomMutatorInterfaces<Schema, typeof mutators>;
  expectTypeOf<MutatorsInterface>().toEqualTypeOf<{
    readonly issue: {
      readonly setTitle: (id: string, title: string) => void;
      readonly setProps: (
        id: string,
        title: string,
        status: 'closed' | 'open',
        assignee: string,
      ) => void;
    };
    readonly nonTableNamespace: {
      readonly doThing: (arg1: string, arg2: number) => void;
    };
  }>();
});
