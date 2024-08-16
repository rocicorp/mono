import {expect, test} from 'vitest';
import {Join} from './join.js';
import {MemorySource, SourceChange} from './memory-source.js';
import {MemoryStorage} from './memory-storage.js';
import {Snarf, expandNode} from './snarf.js';
import type {Row} from './data.js';
import type {HydrateRequest, Output, Source} from './operator.js';

test('hydrate one:many', () => {
  const cases: {
    issues: Row[];
    comments: Row[];
    expected: unknown[];
    expectedCounts: Record<string, number>;
    expectedChildCalls: unknown[];
  }[] = [
    // no data
    {
      issues: [],
      comments: [],
      expected: [],
      expectedCounts: {},
      expectedChildCalls: [],
    },
    // no parent
    {
      issues: [],
      comments: [{id: 'c1', issueID: 'i1'}],
      expected: [],
      expectedCounts: {},
      expectedChildCalls: [],
    },
    // parent, no children
    {
      issues: [{id: 'i1'}],
      comments: [],
      expected: [{row: {id: 'i1'}, relationships: {comments: []}}],
      expectedCounts: {i1: 1},
      expectedChildCalls: [
        ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ],
    },
    // one parent, one child
    {
      issues: [{id: 'i1'}],
      comments: [{id: 'c1', issueID: 'i1'}],
      expected: [
        {
          row: {id: 'i1'},
          relationships: {
            comments: [{row: {id: 'c1', issueID: 'i1'}, relationships: {}}],
          },
        },
      ],
      expectedCounts: {i1: 1},
      expectedChildCalls: [
        ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ],
    },
    // one parent, wrong child
    {
      issues: [{id: 'i1'}],
      comments: [{id: 'c1', issueID: 'i2'}],
      expected: [
        {
          row: {id: 'i1'},
          relationships: {
            comments: [],
          },
        },
      ],
      expectedCounts: {i1: 1},
      expectedChildCalls: [
        ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ],
    },
    // one parent, one child + one wrong child
    {
      issues: [{id: 'i1'}],
      comments: [
        {id: 'c2', issueID: 'i2'},
        {id: 'c1', issueID: 'i1'},
      ],
      expected: [
        {
          row: {id: 'i1'},
          relationships: {
            comments: [{row: {id: 'c1', issueID: 'i1'}, relationships: {}}],
          },
        },
      ],
      expectedCounts: {i1: 1},
      expectedChildCalls: [
        ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ],
    },
    // two parents, each with two children
    {
      issues: [{id: 'i2'}, {id: 'i1'}],
      comments: [
        {id: 'c4', issueID: 'i2'},
        {id: 'c3', issueID: 'i2'},
        {id: 'c2', issueID: 'i1'},
        {id: 'c1', issueID: 'i1'},
      ],
      expected: [
        {
          row: {id: 'i1'},
          relationships: {
            comments: [
              {row: {id: 'c1', issueID: 'i1'}, relationships: {}},
              {row: {id: 'c2', issueID: 'i1'}, relationships: {}},
            ],
          },
        },
        {
          row: {id: 'i2'},
          relationships: {
            comments: [
              {row: {id: 'c3', issueID: 'i2'}, relationships: {}},
              {row: {id: 'c4', issueID: 'i2'}, relationships: {}},
            ],
          },
        },
      ],
      expectedCounts: {i1: 1, i2: 1},
      expectedChildCalls: [
        ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
        ['hydrate', {constraint: {key: 'issueID', value: 'i2'}}],
      ],
    },
  ];

  for (const c of cases) {
    const parent = new SpySource(new MemorySource([['id', 'asc']]));
    const child = new SpySource(new MemorySource([['id', 'asc']]));

    for (const row of c.issues) {
      parent.push({type: 'add', row});
    }
    for (const row of c.comments) {
      child.push({type: 'add', row});
    }

    parent.calls.length = 0;
    child.calls.length = 0;

    const storage = new MemoryStorage();
    const join = new Join(parent, child, storage, 'id', 'issueID', 'comments');
    const snarf = new Snarf();
    join.setOutput(snarf);

    const r = [...join.hydrate({}, snarf)].map(expandNode);
    expect(r).toEqual(c.expected);
    expect(snarf.changes).toEqual([]);
    expect(storage.cloneData()).toEqual(
      Object.fromEntries(
        Object.entries(c.expectedCounts).map(([k, v]) => [
          JSON.stringify(['hydrate-count', k]),
          v,
        ]),
      ),
    );
    expect(parent.calls).toEqual([['hydrate', {}]]);
    expect(child.calls).toEqual(c.expectedChildCalls);
  }
});

test('hydrate many:one', () => {
  const cases: {
    issues: Row[];
    users: Row[];
    expected: unknown[];
    expectedCounts: Record<string, number>;
    expectedChildCalls: unknown[];
  }[] = [
    // no data
    {
      issues: [],
      users: [],
      expected: [],
      expectedCounts: {},
      expectedChildCalls: [],
    },
    // one parent, no child
    // we do not enforce FKs at the zql level - this is a valid state
    {
      issues: [{id: 'i1', ownerID: 'u1'}],
      users: [],
      expected: [{row: {id: 'i1', ownerID: 'u1'}, relationships: {owner: []}}],
      expectedCounts: {u1: 1},
      expectedChildCalls: [['hydrate', {constraint: {key: 'id', value: 'u1'}}]],
    },
    // no parent, one child
    {
      issues: [],
      users: [{id: 'u1'}],
      expected: [],
      expectedCounts: {},
      expectedChildCalls: [],
    },
    // one parent, one child
    {
      issues: [{id: 'i1', ownerID: 'u1'}],
      users: [{id: 'u1'}],
      expected: [
        {
          row: {id: 'i1', ownerID: 'u1'},
          relationships: {
            owner: [{row: {id: 'u1'}, relationships: {}}],
          },
        },
      ],
      expectedCounts: {u1: 1},
      expectedChildCalls: [['hydrate', {constraint: {key: 'id', value: 'u1'}}]],
    },
    // two parents, one child
    {
      issues: [
        {id: 'i2', ownerID: 'u1'},
        {id: 'i1', ownerID: 'u1'},
      ],
      users: [{id: 'u1'}],
      expected: [
        {
          row: {id: 'i1', ownerID: 'u1'},
          relationships: {
            owner: [{row: {id: 'u1'}, relationships: {}}],
          },
        },
        {
          row: {id: 'i2', ownerID: 'u1'},
          relationships: {
            owner: [{row: {id: 'u1'}, relationships: {}}],
          },
        },
      ],
      expectedCounts: {u1: 2},
      expectedChildCalls: [['hydrate', {constraint: {key: 'id', value: 'u1'}}]],
    },
    // two parents, two children
    {
      issues: [
        {id: 'i2', ownerID: 'u2'},
        {id: 'i1', ownerID: 'u1'},
      ],
      users: [{id: 'u2'}, {id: 'u1'}],
      expected: [
        {
          row: {id: 'i1', ownerID: 'u1'},
          relationships: {
            owner: [{row: {id: 'u1'}, relationships: {}}],
          },
        },
        {
          row: {id: 'i2', ownerID: 'u2'},
          relationships: {
            owner: [{row: {id: 'u2'}, relationships: {}}],
          },
        },
      ],
      expectedCounts: {u1: 1, u2: 1},
      expectedChildCalls: [
        {constraint: {key: 'id', value: 'u1'}},
        {constraint: {key: 'id', value: 'u2'}},
      ],
    },
  ];

  for (const c of cases) {
    const parent = new SpySource(new MemorySource([['id', 'asc']]));
    const child = new SpySource(new MemorySource([['id', 'asc']]));

    for (const row of c.issues) {
      parent.push({type: 'add', row});
    }
    for (const row of c.users) {
      child.push({type: 'add', row});
    }

    const storage = new MemoryStorage();
    const join = new Join(parent, child, storage, 'ownerID', 'id', 'owner');
    const snarf = new Snarf();
    join.setOutput(snarf);

    const r = [...join.hydrate({}, snarf)].map(expandNode);
    expect(r).toEqual(c.expected);
  }
});

test('hydrate one:many:many', () => {
  const cases: {
    issues: Row[];
    comments: Row[];
    revisions: Row[];
    expected: unknown[];
    expectedIssueIDCounts: Record<string, number>;
    expectedCommentIDCounts: Record<string, number>;
    expectedCommentCalls: unknown[];
    expectedRevisionCalls: unknown[];
  }[] = [
    // no data
    {
      issues: [],
      comments: [],
      revisions: [],
      expected: [],
      expectedIssueIDCounts: {},
      expectedCommentIDCounts: {},
      expectedCommentCalls: [],
      expectedRevisionCalls: [],
    },
    // no parent
    {
      issues: [],
      comments: [{id: 'c1', issueID: 'i1'}],
      revisions: [{id: 'r1', commentID: 'c1'}],
      expected: [],
      expectedIssueIDCounts: {},
      expectedCommentIDCounts: {},
      expectedCommentCalls: [],
      expectedRevisionCalls: [],
    },
    // one issue, no comments or revisions
    {
      issues: [{id: 'i1'}],
      comments: [],
      revisions: [],
      expected: [{row: {id: 'i1'}, relationships: {comments: []}}],
      expectedIssueIDCounts: {i1: 1},
      expectedCommentIDCounts: {},
      expectedCommentCalls: [
        ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ],
      expectedRevisionCalls: [],
    },
    // one issue, one comment, no revisions
    {
      issues: [{id: 'i1'}],
      comments: [{id: 'c1', issueID: 'i1'}],
      revisions: [],
      expected: [
        {
          row: {id: 'i1'},
          relationships: {
            comments: [
              {
                row: {id: 'c1', issueID: 'i1'},
                relationships: {
                  revisions: [],
                },
              },
            ],
          },
        },
      ],
      expectedIssueIDCounts: {i1: 1},
      expectedCommentIDCounts: {c1: 1},
      expectedCommentCalls: [
        ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ],
      expectedRevisionCalls: [
        ['hydrate', {constraint: {key: 'commentID', value: 'c1'}}],
      ],
    },
    // one issue, one comment, one revision
    {
      issues: [{id: 'i1'}],
      comments: [{id: 'c1', issueID: 'i1'}],
      revisions: [{id: 'r1', commentID: 'c1'}],
      expected: [
        {
          row: {id: 'i1'},
          relationships: {
            comments: [
              {
                row: {id: 'c1', issueID: 'i1'},
                relationships: {
                  revisions: [
                    {row: {id: 'r1', commentID: 'c1'}, relationships: {}},
                  ],
                },
              },
            ],
          },
        },
      ],
      expectedIssueIDCounts: {i1: 1},
      expectedCommentIDCounts: {c1: 1},
      expectedCommentCalls: [
        ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ],
      expectedRevisionCalls: [
        ['hydrate', {constraint: {key: 'commentID', value: 'c1'}}],
      ],
    },
    // two issues, four comments, eight revisions
    {
      issues: [{id: 'i1'}, {id: 'i2'}],
      comments: [
        {id: 'c1', issueID: 'i1'},
        {id: 'c2', issueID: 'i1'},
        {id: 'c3', issueID: 'i2'},
        {id: 'c4', issueID: 'i2'},
      ],
      revisions: [
        {id: 'r1', commentID: 'c1'},
        {id: 'r2', commentID: 'c1'},
        {id: 'r3', commentID: 'c2'},
        {id: 'r4', commentID: 'c2'},
        {id: 'r5', commentID: 'c3'},
        {id: 'r6', commentID: 'c3'},
        {id: 'r7', commentID: 'c4'},
        {id: 'r8', commentID: 'c4'},
      ],
      expected: [
        {
          row: {id: 'i1'},
          relationships: {
            comments: [
              {
                row: {id: 'c1', issueID: 'i1'},
                relationships: {
                  revisions: [
                    {row: {id: 'r1', commentID: 'c1'}, relationships: {}},
                    {row: {id: 'r2', commentID: 'c1'}, relationships: {}},
                  ],
                },
              },
              {
                row: {id: 'c2', issueID: 'i1'},
                relationships: {
                  revisions: [
                    {row: {id: 'r3', commentID: 'c2'}, relationships: {}},
                    {row: {id: 'r4', commentID: 'c2'}, relationships: {}},
                  ],
                },
              },
            ],
          },
        },
        {
          row: {id: 'i2'},
          relationships: {
            comments: [
              {
                row: {id: 'c3', issueID: 'i2'},
                relationships: {
                  revisions: [
                    {row: {id: 'r5', commentID: 'c3'}, relationships: {}},
                    {row: {id: 'r6', commentID: 'c3'}, relationships: {}},
                  ],
                },
              },
              {
                row: {id: 'c4', issueID: 'i2'},
                relationships: {
                  revisions: [
                    {row: {id: 'r7', commentID: 'c4'}, relationships: {}},
                    {row: {id: 'r8', commentID: 'c4'}, relationships: {}},
                  ],
                },
              },
            ],
          },
        },
      ],
      expectedIssueIDCounts: {i1: 1, i2: 1},
      expectedCommentIDCounts: {c1: 1, c2: 1, c3: 1, c4: 1},
      expectedCommentCalls: [
        ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
        ['hydrate', {constraint: {key: 'issueID', value: 'i2'}}],
      ],
      expectedRevisionCalls: [
        ['hydrate', {constraint: {key: 'commentID', value: 'c1'}}],
        ['hydrate', {constraint: {key: 'commentID', value: 'c2'}}],
        ['hydrate', {constraint: {key: 'commentID', value: 'c3'}}],
        ['hydrate', {constraint: {key: 'commentID', value: 'c4'}}],
      ],
    },
  ];

  for (const c of cases) {
    const issue = new SpySource(new MemorySource([['id', 'asc']]));
    const comment = new SpySource(new MemorySource([['id', 'asc']]));
    const revision = new SpySource(new MemorySource([['id', 'asc']]));

    for (const row of c.issues) {
      issue.push({type: 'add', row});
    }
    for (const row of c.comments) {
      comment.push({type: 'add', row});
    }
    for (const row of c.revisions) {
      revision.push({type: 'add', row});
    }

    issue.calls.length = 0;
    comment.calls.length = 0;
    revision.calls.length = 0;

    const s1 = new MemoryStorage();
    const s2 = new MemoryStorage();
    const j1 = new Join(comment, revision, s1, 'id', 'commentID', 'revisions');
    const j2 = new Join(issue, j1, s2, 'id', 'issueID', 'comments');
    const sn1 = new Snarf();
    const sn2 = new Snarf();
    j1.setOutput(sn1);
    j2.setOutput(sn2);

    const r = [...j2.hydrate({}, sn2)].map(expandNode);
    expect(r).toEqual(c.expected);
    expect(sn1.changes).toEqual([]);
    expect(sn2.changes).toEqual([]);
    expect(s1.cloneData()).toEqual(
      Object.fromEntries(
        Object.entries(c.expectedCommentIDCounts).map(([k, v]) => [
          JSON.stringify(['hydrate-count', k]),
          v,
        ]),
      ),
    );
    expect(s2.cloneData()).toEqual(
      Object.fromEntries(
        Object.entries(c.expectedIssueIDCounts).map(([k, v]) => [
          JSON.stringify(['hydrate-count', k]),
          v,
        ]),
      ),
    );
    expect(issue.calls).toEqual([['hydrate', {}]]);
    expect(comment.calls).toEqual(c.expectedCommentCalls);
    expect(revision.calls).toEqual(c.expectedRevisionCalls);
  }
});

test('hydrate one:many:one', () => {
  const cases: {
    issues: Row[];
    issueLabels: Row[];
    labels: Row[];
    expected: unknown[];
    expectedJ1ParentKeyCounts: Record<string, number>;
    expectedJ2ParentKeyCounts: Record<string, number>;
    expectedIssueLabelCalls: unknown[];
    expectedLabelCalls: unknown[];
  }[] = [
    // no data
    {
      issues: [],
      issueLabels: [],
      labels: [],
      expected: [],
      expectedJ1ParentKeyCounts: {},
      expectedJ2ParentKeyCounts: {},
      expectedIssueLabelCalls: [],
      expectedLabelCalls: [],
    },
    // no issues
    {
      issues: [],
      issueLabels: [{issueID: 'i1', labelID: 'l1'}],
      labels: [{id: 'l1'}],
      expected: [],
      expectedJ1ParentKeyCounts: {},
      expectedJ2ParentKeyCounts: {},
      expectedIssueLabelCalls: [],
      expectedLabelCalls: [],
    },
    // one issue, no issuelabels, no labels
    {
      issues: [{id: 'i1'}],
      issueLabels: [],
      labels: [],
      expected: [{row: {id: 'i1'}, relationships: {issuelabels: []}}],
      expectedJ1ParentKeyCounts: {i1: 1},
      expectedJ2ParentKeyCounts: {},
      expectedIssueLabelCalls: [
        ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ],
      expectedLabelCalls: [],
    },
    // one issue, one issuelabel, no labels
    {
      issues: [{id: 'i1'}],
      issueLabels: [{issueID: 'i1', labelID: 'l1'}],
      labels: [],
      expected: [
        {
          row: {id: 'i1'},
          relationships: {
            issuelabels: [
              {
                row: {issueID: 'i1', labelID: 'l1'},
                relationships: {labels: []},
              },
            ],
          },
        },
      ],
      expectedJ1ParentKeyCounts: {i1: 1},
      expectedJ2ParentKeyCounts: {l1: 1},
      expectedIssueLabelCalls: [
        ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ],
      expectedLabelCalls: [['hydrate', {constraint: {key: 'id', value: 'l1'}}]],
    },
    // one issue, one issuelabel, one label
    {
      issues: [{id: 'i1'}],
      issueLabels: [{issueID: 'i1', labelID: 'l1'}],
      labels: [{id: 'l1'}],
      expected: [
        {
          row: {id: 'i1'},
          relationships: {
            issuelabels: [
              {
                row: {issueID: 'i1', labelID: 'l1'},
                relationships: {labels: [{row: {id: 'l1'}, relationships: {}}]},
              },
            ],
          },
        },
      ],
      expectedJ1ParentKeyCounts: {i1: 1},
      expectedJ2ParentKeyCounts: {l1: 1},
      expectedIssueLabelCalls: [
        ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ],
      expectedLabelCalls: [['hydrate', {constraint: {key: 'id', value: 'l1'}}]],
    },
    // one issue, two issuelabels, two labels
    {
      issues: [{id: 'i1'}],
      issueLabels: [
        {issueID: 'i1', labelID: 'l1'},
        {issueID: 'i1', labelID: 'l2'},
      ],
      labels: [{id: 'l1'}, {id: 'l2'}],
      expected: [
        {
          row: {id: 'i1'},
          relationships: {
            issuelabels: [
              {
                row: {issueID: 'i1', labelID: 'l1'},
                relationships: {labels: [{row: {id: 'l1'}, relationships: {}}]},
              },
              {
                row: {issueID: 'i1', labelID: 'l2'},
                relationships: {labels: [{row: {id: 'l2'}, relationships: {}}]},
              },
            ],
          },
        },
      ],
      expectedJ1ParentKeyCounts: {i1: 1},
      expectedJ2ParentKeyCounts: {l1: 1, l2: 1},
      expectedIssueLabelCalls: [
        ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
      ],
      expectedLabelCalls: [
        ['hydrate', {constraint: {key: 'id', value: 'l1'}}],
        ['hydrate', {constraint: {key: 'id', value: 'l2'}}],
      ],
    },
    // two issues, four issuelabels, two labels
    {
      issues: [{id: 'i1'}, {id: 'i2'}],
      issueLabels: [
        {issueID: 'i1', labelID: 'l1'},
        {issueID: 'i1', labelID: 'l2'},
        {issueID: 'i2', labelID: 'l1'},
        {issueID: 'i2', labelID: 'l2'},
      ],
      labels: [{id: 'l1'}, {id: 'l2'}],
      expected: [
        {
          row: {id: 'i1'},
          relationships: {
            issuelabels: [
              {
                row: {issueID: 'i1', labelID: 'l1'},
                relationships: {labels: [{row: {id: 'l1'}, relationships: {}}]},
              },
              {
                row: {issueID: 'i1', labelID: 'l2'},
                relationships: {labels: [{row: {id: 'l2'}, relationships: {}}]},
              },
            ],
          },
        },
        {
          row: {id: 'i2'},
          relationships: {
            issuelabels: [
              {
                row: {issueID: 'i2', labelID: 'l1'},
                relationships: {labels: [{row: {id: 'l1'}, relationships: {}}]},
              },
              {
                row: {issueID: 'i2', labelID: 'l2'},
                relationships: {labels: [{row: {id: 'l2'}, relationships: {}}]},
              },
            ],
          },
        },
      ],
      expectedJ1ParentKeyCounts: {i1: 1, i2: 1},
      expectedJ2ParentKeyCounts: {l1: 2, l2: 2},
      expectedIssueLabelCalls: [
        ['hydrate', {constraint: {key: 'issueID', value: 'i1'}}],
        ['hydrate', {constraint: {key: 'issueID', value: 'i2'}}],
      ],
      expectedLabelCalls: [
        ['hydrate', {constraint: {key: 'id', value: 'l1'}}],
        ['hydrate', {constraint: {key: 'id', value: 'l2'}}],
        ['fetch', {constraint: {key: 'id', value: 'l1'}}],
        ['fetch', {constraint: {key: 'id', value: 'l2'}}],
      ],
    },
  ];

  for (const c of cases) {
    const issueSource = new SpySource(new MemorySource([['id', 'asc']]));
    const issueLabelSource = new SpySource(
      new MemorySource([
        ['issueID', 'asc'],
        ['labelID', 'asc'],
      ]),
    );
    const labelSource = new SpySource(new MemorySource([['id', 'asc']]));

    for (const row of c.issues) {
      issueSource.push({type: 'add', row});
    }
    for (const row of c.issueLabels) {
      issueLabelSource.push({type: 'add', row});
    }
    for (const row of c.labels) {
      labelSource.push({type: 'add', row});
    }

    issueSource.calls.length = 0;
    issueLabelSource.calls.length = 0;
    labelSource.calls.length = 0;

    const s1 = new MemoryStorage();
    const s2 = new MemoryStorage();
    const j2 = new Join(
      issueLabelSource,
      labelSource,
      s2,
      'labelID',
      'id',
      'labels',
    );
    const j1 = new Join(issueSource, j2, s1, 'id', 'issueID', 'issuelabels');
    const sn1 = new Snarf();
    const sn2 = new Snarf();
    j1.setOutput(sn1);
    j2.setOutput(sn2);

    const r = [...j1.hydrate({}, sn1)].map(expandNode);
    expect(r).toEqual(c.expected);
    expect(sn1.changes).toEqual([]);
    expect(sn2.changes).toEqual([]);
    expect(s1.cloneData()).toEqual(
      Object.fromEntries(
        Object.entries(c.expectedJ1ParentKeyCounts).map(([k, v]) => [
          JSON.stringify(['hydrate-count', k]),
          v,
        ]),
      ),
    );
    expect(s2.cloneData()).toEqual(
      Object.fromEntries(
        Object.entries(c.expectedJ2ParentKeyCounts).map(([k, v]) => [
          JSON.stringify(['hydrate-count', k]),
          v,
        ]),
      ),
    );
    expect(issueSource.calls).toEqual([['hydrate', {}]]);
    expect(issueLabelSource.calls).toEqual(c.expectedIssueLabelCalls);
    expect(labelSource.calls).toEqual(c.expectedLabelCalls);
  }
});

// Using a custom spy instead of vi.spyOn because I want to see the order of
// calls to various methods.
class SpySource implements Source {
  #spied: MemorySource;

  readonly calls: unknown[];

  constructor(spied: MemorySource) {
    this.#spied = spied;
    this.calls = [];
  }

  get schema() {
    return this.#spied.schema;
  }

  addOutput(output: Output) {
    this.#spied.addOutput(output);
  }

  hydrate(req: HydrateRequest, output: Output) {
    this.calls.push(['hydrate', req]);
    return this.#spied.hydrate(req, output);
  }

  fetch(req: HydrateRequest, output: Output) {
    this.calls.push(['fetch', req]);
    return this.#spied.fetch(req, output);
  }

  dehydrate(req: HydrateRequest, output: Output) {
    this.calls.push(['dehydrate', req]);
    return this.#spied.dehydrate(req, output);
  }

  push(change: SourceChange) {
    this.calls.push(change);
    this.#spied.push(change);
  }
}

// push
// fetch
// dehydrate
