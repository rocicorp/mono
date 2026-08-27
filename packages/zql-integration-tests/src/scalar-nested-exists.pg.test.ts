import {test} from 'vitest';
import {relationships} from '../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../zero-schema/src/builder/table-builder.ts';
import {createVitests} from './helpers/runner.ts';

/**
 * Repro for a user report: a `whereExists(oneRel, cb, {scalar: true})` whose
 * callback contains a *non-scalar* exists over a many-relation silently
 * returns the wrong rows (usually zero) through z2s, while the IVM paths
 * (memory + zqlite) return the correct rows.
 *
 * The scalar rewrite replaces the EXISTS with
 *
 *   parentField = (SELECT childField FROM child WHERE <cb> ORDER BY <pk> LIMIT 1)
 *
 * which is only equivalent to the EXISTS when the subquery matches at most one
 * row *globally* — i.e. when the callback pins a unique key with literal
 * equalities. That is the contract zqlite's `resolveSimpleScalarSubqueries`
 * enforces (`isSimpleSubquery`) before applying the same rewrite on the IVM
 * side; when the subquery is not "simple" the IVM leaves the condition as a
 * plain EXISTS. z2s applies the rewrite unconditionally, so for a non-simple
 * subquery it compares against an arbitrary (lowest-pk) matching child row and
 * drops every parent that points at a different one.
 *
 * Schema mirrors the report: event → post (many-to-one), post → team
 * (many-to-one), team ← teamMember (one-to-many).
 */

const team = table('team')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const teamMember = table('teamMember')
  .columns({
    id: string(),
    teamId: string(),
    memberId: string(),
  })
  .primaryKey('id');

const post = table('post')
  .columns({
    id: string(),
    teamId: string(),
    title: string(),
  })
  .primaryKey('id');

const event = table('event')
  .columns({
    id: string(),
    rootPostId: string(),
    sortPath: string(),
  })
  .primaryKey('id');

const teamRelationships = relationships(team, ({many}) => ({
  teamMembers: many({
    sourceField: ['id'],
    destField: ['teamId'],
    destSchema: teamMember,
  }),
}));

const postRelationships = relationships(post, ({one}) => ({
  team: one({
    sourceField: ['teamId'],
    destField: ['id'],
    destSchema: team,
  }),
}));

const eventRelationships = relationships(event, ({one}) => ({
  rootPost: one({
    sourceField: ['rootPostId'],
    destField: ['id'],
    destSchema: post,
  }),
}));

const schema = createSchema({
  tables: [team, teamMember, post, event],
  relationships: [teamRelationships, postRelationships, eventRelationships],
});

const pgContent = /*sql*/ `
CREATE TABLE "team" (
  "id" TEXT PRIMARY KEY,
  "name" TEXT NOT NULL
);

CREATE TABLE "teamMember" (
  "id" TEXT PRIMARY KEY,
  "teamId" TEXT NOT NULL,
  "memberId" TEXT NOT NULL
);

CREATE TABLE "post" (
  "id" TEXT PRIMARY KEY,
  "teamId" TEXT NOT NULL,
  "title" TEXT NOT NULL
);

CREATE TABLE "event" (
  "id" TEXT PRIMARY KEY,
  "rootPostId" TEXT NOT NULL,
  "sortPath" TEXT NOT NULL
);
`;

const MEMBER_ID = 'member1';

/**
 * `member1` belongs to both teams, so both posts pass the access chain.
 * `post1` sorts first, which is the row the scalar rewrite's `LIMIT 1`
 * latches onto — so every event rooted at `post2` is silently dropped.
 */
const testData = () => ({
  team: [
    {id: 'team1', name: 'Team 1'},
    {id: 'team2', name: 'Team 2'},
  ],
  teamMember: [
    {id: 'tm1', teamId: 'team1', memberId: MEMBER_ID},
    {id: 'tm2', teamId: 'team2', memberId: MEMBER_ID},
  ],
  post: [
    {id: 'post1', teamId: 'team1', title: 'Post 1'},
    {id: 'post2', teamId: 'team2', title: 'Post 2'},
  ],
  event: [
    {id: 'event1', rootPostId: 'post1', sortPath: 'a'},
    {id: 'event2', rootPostId: 'post2', sortPath: 'b'},
  ],
});

// oxlint-disable-next-line expect-expect
test.each(
  await createVitests(
    {
      suiteName: 'scalar_nested_exists',
      pgContent,
      zqlSchema: schema,
      testData,
      push: 0,
    },
    [
      {
        // The user's query, verbatim in shape. z2s returns [], IVM returns
        // event2.
        name: 'scalar exists nesting a many-relation exists',
        // The reported bug, verbatim — and the static check now rejects it at
        // authoring time, since the callback pins no unique key of `post`.
        // Kept as a runtime case to prove the compiled SQL is correct.
        createQuery: q =>
          q.event
            .where('rootPostId', '=', 'post2')
            .whereExists(
              'rootPost',
              p =>
                p.whereExists('team', t =>
                  t.whereExists('teamMembers', m =>
                    m.where('memberId', '=', MEMBER_ID),
                  ),
                ),
              {
                // @ts-expect-error deliberately unpinned
                scalar: true,
              },
            )
            .orderBy('sortPath', 'asc')
            .limit(20),
        manualVerification: [
          {id: 'event2', rootPostId: 'post2', sortPath: 'b'},
        ],
      },
      {
        // Identical, minus `{scalar: true}` — the user's workaround. Correct
        // everywhere.
        name: 'non-scalar exists nesting a many-relation exists',
        createQuery: q =>
          q.event
            .where('rootPostId', '=', 'post2')
            .whereExists('rootPost', p =>
              p.whereExists('team', t =>
                t.whereExists('teamMembers', m =>
                  m.where('memberId', '=', MEMBER_ID),
                ),
              ),
            )
            .orderBy('sortPath', 'asc')
            .limit(20),
        manualVerification: [
          {id: 'event2', rootPostId: 'post2', sortPath: 'b'},
        ],
      },
      {
        // The user's second data point: an inner primary-key equality makes
        // the subquery "simple", so the scalar rewrite is sound and z2s
        // agrees with IVM.
        name: 'scalar exists with an inner primary-key equality',
        createQuery: q =>
          q.event
            .where('rootPostId', '=', 'post2')
            .whereExists(
              'rootPost',
              p =>
                p
                  .where('id', '=', 'post2')
                  .whereExists('team', t =>
                    t.whereExists('teamMembers', m =>
                      m.where('memberId', '=', MEMBER_ID),
                    ),
                  ),
              {scalar: true},
            )
            .orderBy('sortPath', 'asc')
            .limit(20),
        manualVerification: [
          {id: 'event2', rootPostId: 'post2', sortPath: 'b'},
        ],
      },
      {
        // `NOT EXISTS` + scalar, with a "simple" (pk-pinned) subquery. z2s
        // emits `x IS NOT (SELECT …)`, which is not valid Postgres syntax —
        // a separate bug from the one above, and one the existing
        // compiler.output snapshot bakes in without ever executing it.
        name: 'scalar not-exists with an inner primary-key equality',
        createQuery: q =>
          q.event
            .where(({not, exists}) =>
              not(
                exists('rootPost', p => p.where('id', '=', 'post1'), {
                  scalar: true,
                }),
              ),
            )
            .orderBy('sortPath', 'asc'),
        manualVerification: [
          {id: 'event2', rootPostId: 'post2', sortPath: 'b'},
        ],
      },
      {
        // The nested exists is not the only trigger: any callback that fails
        // to pin a unique key breaks the same way. Both posts match the LIKE,
        // and the rewrite latches onto post1.
        name: 'scalar exists with a non-unique simple filter',
        createQuery: q =>
          q.event
            .where('rootPostId', '=', 'post2')
            .whereExists('rootPost', p => p.where('title', 'LIKE', 'Post%'), {
              // @ts-expect-error deliberately unpinned — `title` pins no unique key
              scalar: true,
            })
            .orderBy('sortPath', 'asc'),
        manualVerification: [
          {id: 'event2', rootPostId: 'post2', sortPath: 'b'},
        ],
      },
    ],
  ),
)('$name', async ({fn}) => {
  await fn();
});
