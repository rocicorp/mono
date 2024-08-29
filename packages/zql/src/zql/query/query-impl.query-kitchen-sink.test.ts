import {describe, test} from 'vitest';
import {newQuery} from './query-impl.js';
import {MemoryStorage} from '../ivm2/memory-storage.js';
import {MemorySource} from '../ivm2/memory-source.js';
import {
  commentSchema,
  issueLabelSchema,
  issueSchema,
  labelSchema,
  revisionSchema,
  userSchema,
} from './test/testSchemas.js';
import {toInputArgs} from './schema.js';
import {must} from 'shared/src/must.js';
import {Host} from '../builder/builder.js';
import {SubscriptionDelegate} from '../context/context.js';

function makeSources() {
  const userArgs = toInputArgs(userSchema);
  const issueArgs = toInputArgs(issueSchema);
  const commentArgs = toInputArgs(commentSchema);
  const revisionArgs = toInputArgs(revisionSchema);
  const labelArgs = toInputArgs(labelSchema);
  const issueLabelArgs = toInputArgs(issueLabelSchema);
  return {
    user: new MemorySource('user', userArgs.columns, userArgs.primaryKey),
    issue: new MemorySource('issue', issueArgs.columns, issueArgs.primaryKey),
    comment: new MemorySource(
      'comment',
      commentArgs.columns,
      commentArgs.primaryKey,
    ),
    revision: new MemorySource(
      'revision',
      revisionArgs.columns,
      revisionArgs.primaryKey,
    ),
    label: new MemorySource('label', labelArgs.columns, labelArgs.primaryKey),
    issueLabel: new MemorySource(
      'issueLabel',
      issueLabelArgs.columns,
      issueLabelArgs.primaryKey,
    ),
  };
}

function makeHost(): Host & SubscriptionDelegate {
  const sources = makeSources();
  return {
    getSource(tableName: string) {
      return must(sources[tableName as keyof typeof sources]);
    },
    createStorage() {
      return new MemoryStorage();
    },
    subscriptionAdded() {
      return () => {};
    },
  };
}

function addData(host: Host) {
  // Add users
  host.getSource('user').push({type: 'add', row: {id: '001', name: 'Alice'}});
  host.getSource('user').push({type: 'add', row: {id: '002', name: 'Bob'}});
  host.getSource('user').push({type: 'add', row: {id: '003', name: 'Charlie'}});

  // Add issues
  host.getSource('issue').push({
    type: 'add',
    row: {
      id: '101',
      title: 'Issue 1',
      description: 'Description 1',
      closed: false,
      ownerId: '001',
    },
  });
  host.getSource('issue').push({
    type: 'add',
    row: {
      id: '102',
      title: 'Issue 2',
      description: 'Description 2',
      closed: true,
      ownerId: '001',
    },
  });
  host.getSource('issue').push({
    type: 'add',
    row: {
      id: '103',
      title: 'Issue 3',
      description: 'Description 3',
      closed: false,
      ownerId: '001',
    },
  });

  host.getSource('issue').push({
    type: 'add',
    row: {
      id: '104',
      title: 'Issue 4',
      description: 'Description 4',
      closed: false,
      ownerId: '002',
    },
  });
  host.getSource('issue').push({
    type: 'add',
    row: {
      id: '105',
      title: 'Issue 5',
      description: 'Description 5',
      closed: true,
      ownerId: '002',
    },
  });
  host.getSource('issue').push({
    type: 'add',
    row: {
      id: '106',
      title: 'Issue 6',
      description: 'Description 6',
      closed: false,
      ownerId: '002',
    },
  });

  host.getSource('issue').push({
    type: 'add',
    row: {
      id: '107',
      title: 'Issue 7',
      description: 'Description 7',
      closed: false,
      ownerId: '003',
    },
  });
  host.getSource('issue').push({
    type: 'add',
    row: {
      id: '108',
      title: 'Issue 8',
      description: 'Description 8',
      closed: true,
      ownerId: '003',
    },
  });
  host.getSource('issue').push({
    type: 'add',
    row: {
      id: '109',
      title: 'Issue 9',
      description: 'Description 9',
      closed: false,
      ownerId: '003',
    },
  });

  // Add comments
  host.getSource('comment').push({
    type: 'add',
    row: {id: '201', issueId: '101', text: 'Comment 1', authorId: '001'},
  });
  host.getSource('comment').push({
    type: 'add',
    row: {id: '202', issueId: '101', text: 'Comment 2', authorId: '002'},
  });
  host.getSource('comment').push({
    type: 'add',
    row: {id: '203', issueId: '101', text: 'Comment 3', authorId: '003'},
  });
  host.getSource('comment').push({
    type: 'add',
    row: {id: '204', issueId: '102', text: 'Comment 4', authorId: '001'},
  });
  host.getSource('comment').push({
    type: 'add',
    row: {id: '205', issueId: '102', text: 'Comment 5', authorId: '002'},
  });
  host.getSource('comment').push({
    type: 'add',
    row: {id: '206', issueId: '102', text: 'Comment 6', authorId: '003'},
  });
  host.getSource('comment').push({
    type: 'add',
    row: {id: '207', issueId: '103', text: 'Comment 7', authorId: '001'},
  });
  host.getSource('comment').push({
    type: 'add',
    row: {id: '208', issueId: '103', text: 'Comment 8', authorId: '002'},
  });
  host.getSource('comment').push({
    type: 'add',
    row: {id: '209', issueId: '103', text: 'Comment 9', authorId: '003'},
  });

  // Add revisions
  host.getSource('revision').push({
    type: 'add',
    row: {id: '301', commentId: '201', text: 'Revision 1', authorId: '001'},
  });
  host.getSource('revision').push({
    type: 'add',
    row: {id: '302', commentId: '201', text: 'Revision 2', authorId: '001'},
  });
  host.getSource('revision').push({
    type: 'add',
    row: {id: '303', commentId: '201', text: 'Revision 3', authorId: '001'},
  });
  host.getSource('revision').push({
    type: 'add',
    row: {id: '304', commentId: '202', text: 'Revision 1', authorId: '002'},
  });
  host.getSource('revision').push({
    type: 'add',
    row: {id: '305', commentId: '202', text: 'Revision 2', authorId: '002'},
  });
  host.getSource('revision').push({
    type: 'add',
    row: {id: '306', commentId: '202', text: 'Revision 1', authorId: '002'},
  });
  host.getSource('revision').push({
    type: 'add',
    row: {id: '307', commentId: '203', text: 'Revision 1', authorId: '003'},
  });
  host.getSource('revision').push({
    type: 'add',
    row: {id: '308', commentId: '203', text: 'Revision 2', authorId: '003'},
  });
  host.getSource('revision').push({
    type: 'add',
    row: {id: '309', commentId: '203', text: 'Revision 1', authorId: '003'},
  });

  // Add labels
  host.getSource('label').push({type: 'add', row: {id: '401', name: 'bug'}});
  host
    .getSource('label')
    .push({type: 'add', row: {id: '402', name: 'feature'}});

  // Add issue labels
  host
    .getSource('issueLabel')
    .push({type: 'add', row: {issueId: '101', labelId: '401'}});
  host
    .getSource('issueLabel')
    .push({type: 'add', row: {issueId: '102', labelId: '401'}});
  host
    .getSource('issueLabel')
    .push({type: 'add', row: {issueId: '102', labelId: '402'}});
}

describe('kitchen sink query', () => {
  test('complex query with filters, limits, and multiple joins', () => {
    const host = makeHost();
    addData(host);

    const issueQuery = newQuery(host, issueSchema)
      .where('ownerId', '=', '001')
      .related('owner', q => q.select('name'))
      .related('comments', q =>
        q
          .select('text')
          .related('revisions', r => r.select('text'))
          .limit(1),
      )
      .related('labels', q => q.select('name'))
      .start({id: '101'})
      .limit(2);

    const view = issueQuery.materialize();
    view.hydrate();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let rows: unknown[] = [];
    view.addListener(data => {
      rows = [...data].map(row => ({
        ...row,
        owner: [...row.owner],
        comments: [...row.comments].map(comment => ({
          ...comment,
          revisions: [...comment.revisions],
        })),
        labels: [...row.labels].map(label => ({
          ...label,
        })),
      }));
    });
    console.log(rows);
  });
});
