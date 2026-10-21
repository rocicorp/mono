/**
 * The named queries and mutators the client scenarios use, over the zql test
 * schema. The Swift runner defines the same names with the same bodies
 * (`swift/Tests/ConformanceTests/ClientApp.swift`); keep the two in step.
 */
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {ApplicationError} from '../../../zero-protocol/src/application-error.ts';
import {defineMutators} from '../../../zql/src/mutate/mutator-registry.ts';
import {defineMutator} from '../../../zql/src/mutate/mutator.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import {
  defineQueries,
  defineQuery,
} from '../../../zql/src/query/query-registry.ts';
import {schema} from '../../../zql/src/query/test/test-schemas.ts';

export {schema};

const zql = createBuilder(schema);

export const queries = defineQueries({
  issues: {
    all: defineQuery(() => zql.issue.orderBy('id', 'asc')),
    open: defineQuery(() =>
      zql.issue
        .where('closed', false)
        .related('comments', q => q.orderBy('id', 'asc'))
        .orderBy('createdAt', 'asc'),
    ),
    byOwner: defineQuery(({args}: {args: string}) =>
      zql.issue.where('ownerId', args).orderBy('id', 'asc'),
    ),
    byID: defineQuery(({args}: {args: string}) =>
      zql.issue.where('id', args).related('owner').one(),
    ),
    firstTwo: defineQuery(() => zql.issue.orderBy('title', 'asc').limit(2)),
  },
  users: {
    all: defineQuery(() => zql.user.orderBy('id', 'asc')),
  },
  // A table with a compound primary key.
  issueLabels: {
    all: defineQuery(() =>
      zql.issueLabel.orderBy('issueId', 'asc').orderBy('labelId', 'asc'),
    ),
  },
});

// The mutators use the schema's CRUD API without a typed transaction.
// oxlint-disable-next-line @typescript-eslint/no-explicit-any
type Tx = any;
type JSONObject = {readonly [key: string]: ReadonlyJSONValue};

function issueRow(args: JSONObject) {
  return {
    id: args['id'],
    title: args['title'] ?? '',
    description: args['description'] ?? '',
    closed: args['closed'] ?? false,
    ownerId: args['ownerId'] ?? null,
    createdAt: args['createdAt'] ?? 1,
  };
}

const issue = {
  create: defineMutator(async ({tx, args}: {tx: Tx; args: JSONObject}) => {
    await tx.mutate.issue.insert(issueRow(args));
  }),
  upsert: defineMutator(async ({tx, args}: {tx: Tx; args: JSONObject}) => {
    await tx.mutate.issue.upsert(issueRow(args));
  }),
  update: defineMutator(async ({tx, args}: {tx: Tx; args: JSONObject}) => {
    await tx.mutate.issue.update(args);
  }),
  delete: defineMutator(async ({tx, args}: {tx: Tx; args: string}) => {
    await tx.mutate.issue.delete({id: args});
  }),
  close: defineMutator(async ({tx, args}: {tx: Tx; args: string}) => {
    await tx.mutate.issue.update({id: args, closed: true});
  }),
  // Reads through the transaction, so a rebase can change what it writes.
  closeAllOpen: defineMutator(async ({tx}: {tx: Tx}) => {
    const open = await tx.run(zql.issue.where('closed', false));
    for (const issue of open) {
      await tx.mutate.issue.update({id: issue.id, closed: true});
    }
  }),
  // Throws on a rebase that finds the issue already there.
  createUnique: defineMutator(
    async ({tx, args}: {tx: Tx; args: JSONObject}) => {
      const existing = await tx.run(
        zql.issue.where('id', args['id'] as string).one(),
      );
      if (existing) {
        throw new ApplicationError('issue exists', {
          details: {id: args['id'] ?? null},
        });
      }
      await tx.mutate.issue.insert(issueRow(args));
    },
  ),
  // Returns data to the caller of the optimistic run.
  createReturning: defineMutator(
    async ({tx, args}: {tx: Tx; args: JSONObject}) => {
      await tx.mutate.issue.insert(issueRow(args));
      return {created: args['id'] ?? null};
    },
  ),
  fail: defineMutator(({args}: {args: string}) => {
    throw new Error(args);
  }),
  // Writes and then throws, so nothing it wrote may stay.
  createThenFail: defineMutator(
    async ({tx, args}: {tx: Tx; args: JSONObject}) => {
      await tx.mutate.issue.insert(issueRow(args));
      throw new ApplicationError('failed after writing');
    },
  ),
};

export const mutators = defineMutators({
  issue,
  comment: {
    add: defineMutator(async ({tx, args}: {tx: Tx; args: JSONObject}) => {
      await tx.mutate.comment.insert({
        id: args['id'],
        authorId: args['authorId'] ?? 'u1',
        issueId: args['issueId'],
        text: args['text'] ?? '',
        createdAt: args['createdAt'] ?? 1,
      });
    }),
  },
});

/** The `issue` mutators alone: another set of mutator names. */
export const issueMutators = defineMutators({issue});
