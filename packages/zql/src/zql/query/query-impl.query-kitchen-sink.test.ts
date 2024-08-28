import {describe, expect, test} from 'vitest';
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
    row: {id: '101', title: 'Issue 1', description: 'Description 1', closed: false, ownerId: '001'},
  });
  host.getSource('issue').push({
    type: 'add',
    row: {id: '102', title: 'Issue 2', description: 'Description 2', closed: true, ownerId: '002'},
  });
  host.getSource('issue').push({
    type: 'add',
    row: {id: '103', title: 'Issue 3', description: 'Description 3', closed: false, ownerId: '001'},
  });

  // Add comments
  host.getSource('comment').push({type: 'add', row: {id: '201', issueId: '101', body: 'Comment 1', authorId: '001'}});
  host.getSource('comment').push({type: 'add', row: {id: '202', issueId: '101', body: 'Comment 2', authorId: '002'}});
  host.getSource('comment').push({type: 'add', row: {id: '203', issueId: '102', body: 'Comment 3', authorId: '001'}});
  host.getSource('comment').push({type: 'add', row: {id: '204', issueId: '103', body: 'Comment 4', authorId: '003'}});

  // Add revisions
  host.getSource('revision').push({type: 'add', row: {id: '301', commentId: '201', text: 'Revision 1', authorId: '001'}});
  host.getSource('revision').push({type: 'add', row: {id: '302', commentId: '201', text: 'Revision 2', authorId: '001'}});
  host.getSource('revision').push({type: 'add', row: {id: '303', commentId: '203', text: 'Revision 1', authorId: '001'}});

  // Add labels
  host.getSource('label').push({type: 'add', row: {id: '401', name: 'bug'}});
  host.getSource('label').push({type: 'add', row: {id: '402', name: 'feature'}});

  // Add issue labels
  host.getSource('issueLabel').push({type: 'add', row: {issueId: '101', labelId: '401'}});
  host.getSource('issueLabel').push({type: 'add', row: {issueId: '102', labelId: '402'}});
  host.getSource('issueLabel').push({type: 'add', row: {issueId: '103', labelId: '401'}});
}

describe('kitchen sink query', () => {
  test.only('complex query with filters, limits, and multiple joins', () => {
    const host = makeHost();
    addData(host);

    const issueQuery = newQuery(host, issueSchema)
      .select('id', 'title', 'closed')
      .where('ownerId', '=', '001')
      .related('owner', q => q.select('name'))
      .related('comments', q => 
        q.select('text')
         .related('revisions', r => r.select('text'))
         .limit(1)
      )
      .related('labels', q => q.select('name'))
      .limit(2);

    const view = issueQuery.materialize();
    view.hydrate();

    console.log(view);
  });
});