import {test} from 'vitest';
import {createTableSQL, schema} from '../../zql/src/query/test/test-schemas.ts';
import {createVitests} from './helpers/runner.ts';

const BASE_TIMESTAMP = 1743127752952;
// oxlint-disable-next-line expect-expect
test.each(
  await createVitests(
    {
      suiteName: 'compiler',
      pgContent: createTableSQL,
      zqlSchema: schema,
      testData: () => ({
        issue: Array.from({length: 3}, (_, i) => ({
          id: `issue${i + 1}`,
          title: `Test Issue ${i + 1}`,
          description: `Description for issue ${i + 1}`,
          closed: i % 2 === 0,
          ownerId: i === 0 ? null : `user${i}`,
          createdAt: new Date(BASE_TIMESTAMP - i * 86400000).getTime(),
        })),
        user: Array.from({length: 3}, (_, i) => ({
          id: `user${i + 1}`,
          name: `User ${i + 1}`,
          metadata:
            i === 0
              ? null
              : {
                  registrar: i % 2 === 0 ? 'github' : 'google',
                  email: `user${i + 1}@example.com`,
                  altContacts: [`alt${i + 1}@example.com`],
                },
        })),
        comment: Array.from({length: 6}, (_, i) => ({
          id: `comment${i + 1}`,
          authorId: `user${(i % 3) + 1}`,
          issueId: `issue${(i % 3) + 1}`,
          text: `Comment ${i + 1} text`,
          createdAt: new Date(BASE_TIMESTAMP - i * 86400000).getTime(),
        })),
        issueLabel: Array.from({length: 4}, (_, i) => ({
          issueId: `issue${(i % 3) + 1}`,
          labelId: `label${(i % 2) + 1}`,
        })),
        label: Array.from({length: 2}, (_, i) => ({
          id: `label${i + 1}`,
          name: `Label ${i + 1}`,
        })),
        revision: Array.from({length: 3}, (_, i) => ({
          id: `revision${i + 1}`,
          authorId: `user${(i % 3) + 1}`,
          commentId: `comment${(i % 4) + 1}`,
          text: `Revised text ${i + 1}`,
        })),
      }),
    },
    [
      {
        name: 'basic where clause',
        createQuery: q => q.issue.where('title', 'Test Issue 1'),
        manualVerification: [
          {
            closed: true,
            createdAt: 1743127752952,
            description: 'Description for issue 1',
            id: 'issue1',
            ownerId: null,
            title: 'Test Issue 1',
          },
        ],
      },
      {
        name: 'related nested in junction related',
        createQuery: q => q.label.related('issues', q => q.related('owner')),
        manualVerification: [
          {
            id: 'label1',
            issues: [
              {
                closed: true,
                createdAt: 1743127752952,
                description: 'Description for issue 1',
                id: 'issue1',
                owner: null,
                ownerId: null,
                title: 'Test Issue 1',
              },
              {
                closed: true,
                createdAt: 1742954952952,
                description: 'Description for issue 3',
                id: 'issue3',
                owner: {
                  id: 'user2',
                  metadata: {
                    altContacts: ['alt2@example.com'],
                    email: 'user2@example.com',
                    registrar: 'google',
                  },
                  name: 'User 2',
                },
                ownerId: 'user2',
                title: 'Test Issue 3',
              },
            ],
            name: 'Label 1',
          },
          {
            id: 'label2',
            issues: [
              {
                closed: true,
                createdAt: 1743127752952,
                description: 'Description for issue 1',
                id: 'issue1',
                owner: null,
                ownerId: null,
                title: 'Test Issue 1',
              },
              {
                closed: false,
                createdAt: 1743041352952,
                description: 'Description for issue 2',
                id: 'issue2',
                owner: {
                  id: 'user1',
                  metadata: null,
                  name: 'User 1',
                },
                ownerId: 'user1',
                title: 'Test Issue 2',
              },
            ],
            name: 'Label 2',
          },
        ],
      },
      {
        name: 'related with filter nested in junction related',
        createQuery: q =>
          q.label.related('issues', q =>
            q.related('comments', q => q.where('text', 'LIKE', '%2%')),
          ),
        manualVerification: [
          {
            id: 'label1',
            issues: [
              {
                closed: true,
                comments: [],
                createdAt: 1743127752952,
                description: 'Description for issue 1',
                id: 'issue1',
                ownerId: null,
                title: 'Test Issue 1',
              },
              {
                closed: true,
                comments: [],
                createdAt: 1742954952952,
                description: 'Description for issue 3',
                id: 'issue3',
                ownerId: 'user2',
                title: 'Test Issue 3',
              },
            ],
            name: 'Label 1',
          },
          {
            id: 'label2',
            issues: [
              {
                closed: true,
                comments: [],
                createdAt: 1743127752952,
                description: 'Description for issue 1',
                id: 'issue1',
                ownerId: null,
                title: 'Test Issue 1',
              },
              {
                closed: false,
                comments: [
                  {
                    authorId: 'user2',
                    createdAt: 1743041352952,
                    id: 'comment2',
                    issueId: 'issue2',
                    text: 'Comment 2 text',
                  },
                ],
                createdAt: 1743041352952,
                description: 'Description for issue 2',
                id: 'issue2',
                ownerId: 'user1',
                title: 'Test Issue 2',
              },
            ],
            name: 'Label 2',
          },
        ],
      },
      {
        name: 'multiple where clauses',
        createQuery: q =>
          q.issue.where('closed', '=', false).where('ownerId', 'IS NOT', null),
        manualVerification: [
          {
            closed: false,
            createdAt: 1743041352952,
            description: 'Description for issue 2',
            id: 'issue2',
            ownerId: 'user1',
            title: 'Test Issue 2',
          },
        ],
      },
      {
        name: 'whereExists with related table',
        createQuery: q =>
          q.issue.whereExists('labels', q => q.where('name', '=', 'bug')),
        manualVerification: [],
      },
      {
        name: 'order by and limit',
        createQuery: q => q.issue.orderBy('title', 'desc').limit(5),
        manualVerification: [
          {
            closed: true,
            createdAt: 1742954952952,
            description: 'Description for issue 3',
            id: 'issue3',
            ownerId: 'user2',
            title: 'Test Issue 3',
          },
          {
            closed: false,
            createdAt: 1743041352952,
            description: 'Description for issue 2',
            id: 'issue2',
            ownerId: 'user1',
            title: 'Test Issue 2',
          },
          {
            closed: true,
            createdAt: 1743127752952,
            description: 'Description for issue 1',
            id: 'issue1',
            ownerId: null,
            title: 'Test Issue 1',
          },
        ],
      },
      {
        name: '1 to 1 foreign key relationship',
        createQuery: q => q.issue.related('owner'),
        manualVerification: [
          {
            closed: true,
            createdAt: 1743127752952,
            description: 'Description for issue 1',
            id: 'issue1',
            owner: undefined,
            ownerId: null,
            title: 'Test Issue 1',
          },
          {
            closed: false,
            createdAt: 1743041352952,
            description: 'Description for issue 2',
            id: 'issue2',
            owner: {
              id: 'user1',
              metadata: null,
              name: 'User 1',
            },
            ownerId: 'user1',
            title: 'Test Issue 2',
          },
          {
            closed: true,
            createdAt: 1742954952952,
            description: 'Description for issue 3',
            id: 'issue3',
            owner: {
              id: 'user2',
              metadata: {
                altContacts: ['alt2@example.com'],
                email: 'user2@example.com',
                registrar: 'google',
              },
              name: 'User 2',
            },
            ownerId: 'user2',
            title: 'Test Issue 3',
          },
        ],
      },
      {
        name: '1 to many foreign key relationship',
        createQuery: q => q.issue.related('comments'),
        manualVerification: [
          {
            closed: true,
            comments: [
              {
                authorId: 'user1',
                createdAt: 1743127752952,
                id: 'comment1',
                issueId: 'issue1',
                text: 'Comment 1 text',
              },
              {
                authorId: 'user1',
                createdAt: 1742868552952,
                id: 'comment4',
                issueId: 'issue1',
                text: 'Comment 4 text',
              },
            ],
            createdAt: 1743127752952,
            description: 'Description for issue 1',
            id: 'issue1',
            ownerId: null,
            title: 'Test Issue 1',
          },
          {
            closed: false,
            comments: [
              {
                authorId: 'user2',
                createdAt: 1743041352952,
                id: 'comment2',
                issueId: 'issue2',
                text: 'Comment 2 text',
              },
              {
                authorId: 'user2',
                createdAt: 1742782152952,
                id: 'comment5',
                issueId: 'issue2',
                text: 'Comment 5 text',
              },
            ],
            createdAt: 1743041352952,
            description: 'Description for issue 2',
            id: 'issue2',
            ownerId: 'user1',
            title: 'Test Issue 2',
          },
          {
            closed: true,
            comments: [
              {
                authorId: 'user3',
                createdAt: 1742954952952,
                id: 'comment3',
                issueId: 'issue3',
                text: 'Comment 3 text',
              },
              {
                authorId: 'user3',
                createdAt: 1742695752952,
                id: 'comment6',
                issueId: 'issue3',
                text: 'Comment 6 text',
              },
            ],
            createdAt: 1742954952952,
            description: 'Description for issue 3',
            id: 'issue3',
            ownerId: 'user2',
            title: 'Test Issue 3',
          },
        ],
      },
      {
        name: 'junction relationship',
        createQuery: q => q.issue.related('labels'),
        manualVerification: [
          {
            closed: true,
            createdAt: 1743127752952,
            description: 'Description for issue 1',
            id: 'issue1',
            labels: [
              {
                id: 'label1',
                name: 'Label 1',
              },
              {
                id: 'label2',
                name: 'Label 2',
              },
            ],
            ownerId: null,
            title: 'Test Issue 1',
          },
          {
            closed: false,
            createdAt: 1743041352952,
            description: 'Description for issue 2',
            id: 'issue2',
            labels: [
              {
                id: 'label2',
                name: 'Label 2',
              },
            ],
            ownerId: 'user1',
            title: 'Test Issue 2',
          },
          {
            closed: true,
            createdAt: 1742954952952,
            description: 'Description for issue 3',
            id: 'issue3',
            labels: [
              {
                id: 'label1',
                name: 'Label 1',
              },
            ],
            ownerId: 'user2',
            title: 'Test Issue 3',
          },
        ],
      },
      {
        name: 'nested related where clauses',
        createQuery: q =>
          q.issue
            .where('closed', '=', false)
            .related('comments', q =>
              q
                .where('text', 'ILIKE', '%2%')
                .where('createdAt', '=', 1743041352952)
                .related('author'),
            ),
        manualVerification: [
          {
            closed: false,
            comments: [
              {
                author: {
                  id: 'user2',
                  metadata: {
                    altContacts: ['alt2@example.com'],
                    email: 'user2@example.com',
                    registrar: 'google',
                  },
                  name: 'User 2',
                },
                authorId: 'user2',
                createdAt: 1743041352952,
                id: 'comment2',
                issueId: 'issue2',
                text: 'Comment 2 text',
              },
            ],
            createdAt: 1743041352952,
            description: 'Description for issue 2',
            id: 'issue2',
            ownerId: 'user1',
            title: 'Test Issue 2',
          },
        ],
      },
      {
        name: 'complex query combining multiple features',
        createQuery: q =>
          q.issue
            .where('closed', '=', false)
            .whereExists('labels', q =>
              q.where('name', 'IN', ['Label 1', 'Label 2']),
            )
            .related('owner')
            .related('comments', q =>
              q.orderBy('createdAt', 'desc').limit(3).related('author'),
            )
            .orderBy('title', 'asc'),
        manualVerification: [
          {
            closed: false,
            comments: [
              {
                author: {
                  id: 'user2',
                  metadata: {
                    altContacts: ['alt2@example.com'],
                    email: 'user2@example.com',
                    registrar: 'google',
                  },
                  name: 'User 2',
                },
                authorId: 'user2',
                createdAt: 1743041352952,
                id: 'comment2',
                issueId: 'issue2',
                text: 'Comment 2 text',
              },
              {
                author: {
                  id: 'user2',
                  metadata: {
                    altContacts: ['alt2@example.com'],
                    email: 'user2@example.com',
                    registrar: 'google',
                  },
                  name: 'User 2',
                },
                authorId: 'user2',
                createdAt: 1742782152952,
                id: 'comment5',
                issueId: 'issue2',
                text: 'Comment 5 text',
              },
            ],
            createdAt: 1743041352952,
            description: 'Description for issue 2',
            id: 'issue2',
            owner: {
              id: 'user1',
              metadata: null,
              name: 'User 1',
            },
            ownerId: 'user1',
            title: 'Test Issue 2',
          },
        ],
      },
    ],
  ),
)('$name', async ({fn}) => {
  await fn();
});
