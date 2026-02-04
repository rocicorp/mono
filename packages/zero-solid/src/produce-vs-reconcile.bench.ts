import {createStore, produce, reconcile} from 'solid-js/store';
import {bench, describe} from 'vitest';

// A symbol used as a key for reconcile to match objects by identity.
const ID_SYM = Symbol('id');

// ---------------------------------------------------------------------------
// Data shape helpers
// ---------------------------------------------------------------------------

interface User {
  [ID_SYM]: string;
  id: number;
  name: string;
  email: string;
}

interface Label {
  [ID_SYM]: string;
  id: number;
  name: string;
  color: string;
}

interface Issue {
  [ID_SYM]: string;
  id: number;
  title: string;
  description: string;
  labels: Label[];
  creator: User;
  assignee: User;
}

function makeUser(id: number): User {
  return {
    [ID_SYM]: `user-${id}`,
    id,
    name: `User ${id}`,
    email: `user${id}@test.com`,
  };
}

function makeLabel(id: number): Label {
  return {
    [ID_SYM]: `label-${id}`,
    id,
    name: `Label ${id}`,
    color: `#${String(id).padStart(6, '0')}`,
  };
}

function makeIssue(id: number): Issue {
  return {
    [ID_SYM]: `issue-${id}`,
    id,
    title: `Issue ${id}`,
    description: `Description for issue ${id}`,
    labels: [
      makeLabel(id * 10 + 1),
      makeLabel(id * 10 + 2),
      makeLabel(id * 10 + 3),
    ],
    creator: makeUser(id * 2),
    assignee: makeUser(id * 2 + 1),
  };
}

function makeTree(count: number): Issue[] {
  return Array.from({length: count}, (_, i) => makeIssue(i + 1));
}

// Deep-clone a tree so mutations to one copy don't affect the other.
function cloneTree(tree: Issue[]): Issue[] {
  return tree.map(issue => ({
    ...issue,
    labels: issue.labels.map(l => ({...l})),
    creator: {...issue.creator},
    assignee: {...issue.assignee},
  }));
}

const ISSUE_COUNT = 500;
const SMALL_COUNT = 200;
const LARGE_COUNT = 2000;

const reconcileMerge = {key: ID_SYM as unknown as string, merge: true};
const reconcileNoMerge = {key: ID_SYM as unknown as string};

// ---------------------------------------------------------------------------
// Benchmarks (500 issues)
// ---------------------------------------------------------------------------

describe('produce vs reconcile', () => {
  describe('edit single leaf field', () => {
    bench('produce', () => {
      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(makeTree(ISSUE_COUNT)),
      });
      setState(
        produce(draft => {
          draft.issues[0].title = 'Changed title';
        }),
      );
    });

    bench('reconcile', () => {
      const base = makeTree(ISSUE_COUNT);
      const next = cloneTree(base);
      next[0] = {...next[0], title: 'Changed title'};

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileMerge));
    });
  });

  describe('add row to list', () => {
    bench('produce', () => {
      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(makeTree(ISSUE_COUNT)),
      });
      setState(
        produce(draft => {
          draft.issues.push(makeIssue(ISSUE_COUNT + 1));
        }),
      );
    });

    bench('reconcile', () => {
      const base = makeTree(ISSUE_COUNT);
      const next = [...cloneTree(base), makeIssue(ISSUE_COUNT + 1)];

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileMerge));
    });
  });

  describe('remove row from list', () => {
    bench('produce', () => {
      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(makeTree(ISSUE_COUNT)),
      });
      setState(
        produce(draft => {
          draft.issues.splice(250, 1);
        }),
      );
    });

    bench('reconcile', () => {
      const base = makeTree(ISSUE_COUNT);
      const next = cloneTree(base);
      next.splice(250, 1);

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileMerge));
    });
  });

  describe('edit nested child (many)', () => {
    bench('produce', () => {
      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(makeTree(ISSUE_COUNT)),
      });
      setState(
        produce(draft => {
          draft.issues[100].labels[1].name = 'Updated label';
        }),
      );
    });

    bench('reconcile', () => {
      const base = makeTree(ISSUE_COUNT);
      const next = cloneTree(base);
      next[100] = {
        ...next[100],
        labels: next[100].labels.map((l, i) =>
          i === 1 ? {...l, name: 'Updated label'} : l,
        ),
      };

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileMerge));
    });
  });

  describe('edit nested child (singular)', () => {
    bench('produce', () => {
      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(makeTree(ISSUE_COUNT)),
      });
      setState(
        produce(draft => {
          draft.issues[200].assignee.name = 'New assignee name';
        }),
      );
    });

    bench('reconcile', () => {
      const base = makeTree(ISSUE_COUNT);
      const next = cloneTree(base);
      next[200] = {
        ...next[200],
        assignee: {...next[200].assignee, name: 'New assignee name'},
      };

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileMerge));
    });
  });

  describe('batch edit 50 rows', () => {
    bench('produce', () => {
      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(makeTree(ISSUE_COUNT)),
      });
      setState(
        produce(draft => {
          for (let i = 0; i < 50; i++) {
            draft.issues[i * 10].title = `Batch-edited ${i}`;
          }
        }),
      );
    });

    bench('reconcile', () => {
      const base = makeTree(ISSUE_COUNT);
      const next = cloneTree(base);
      for (let i = 0; i < 50; i++) {
        next[i * 10] = {...next[i * 10], title: `Batch-edited ${i}`};
      }

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileMerge));
    });
  });
});

// ---------------------------------------------------------------------------
// Small-scale benchmarks (200 issues)
// ---------------------------------------------------------------------------

describe('produce vs reconcile (200 issues)', () => {
  describe('single field edit', () => {
    bench('produce', () => {
      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(makeTree(SMALL_COUNT)),
      });
      setState(
        produce(draft => {
          draft.issues[0].title = 'Changed title';
        }),
      );
    });

    bench('reconcile', () => {
      const base = makeTree(SMALL_COUNT);
      const next = cloneTree(base);
      next[0] = {...next[0], title: 'Changed title'};

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileMerge));
    });
  });

  describe('all issues change', () => {
    bench('produce', () => {
      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(makeTree(SMALL_COUNT)),
      });
      setState(
        produce(draft => {
          for (let i = 0; i < SMALL_COUNT; i++) {
            draft.issues[i].title = `Updated ${i}`;
          }
        }),
      );
    });

    bench('reconcile', () => {
      const base = makeTree(SMALL_COUNT);
      const next = cloneTree(base);
      for (let i = 0; i < SMALL_COUNT; i++) {
        next[i] = {...next[i], title: `Updated ${i}`};
      }

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileMerge));
    });
  });
});

// ---------------------------------------------------------------------------
// Large-scale benchmarks (2000 issues)
// ---------------------------------------------------------------------------

describe('produce vs reconcile (2000 issues)', () => {
  describe('single field edit', () => {
    bench('produce', () => {
      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(makeTree(LARGE_COUNT)),
      });
      setState(
        produce(draft => {
          draft.issues[0].title = 'Changed title';
        }),
      );
    });

    bench('reconcile', () => {
      const base = makeTree(LARGE_COUNT);
      const next = cloneTree(base);
      next[0] = {...next[0], title: 'Changed title'};

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileMerge));
    });
  });

  describe('all issues change', () => {
    bench('produce', () => {
      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(makeTree(LARGE_COUNT)),
      });
      setState(
        produce(draft => {
          for (let i = 0; i < LARGE_COUNT; i++) {
            draft.issues[i].title = `Updated ${i}`;
          }
        }),
      );
    });

    bench('reconcile', () => {
      const base = makeTree(LARGE_COUNT);
      const next = cloneTree(base);
      for (let i = 0; i < LARGE_COUNT; i++) {
        next[i] = {...next[i], title: `Updated ${i}`};
      }

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileMerge));
    });
  });
});

// ---------------------------------------------------------------------------
// reconcile merge:true vs merge:false
// ---------------------------------------------------------------------------

describe('reconcile merge:true vs merge:false (500 issues)', () => {
  describe('single field edit', () => {
    bench('merge:true', () => {
      const base = makeTree(ISSUE_COUNT);
      const next = cloneTree(base);
      next[0] = {...next[0], title: 'Changed title'};

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileMerge));
    });

    bench('merge:false', () => {
      const base = makeTree(ISSUE_COUNT);
      const next = cloneTree(base);
      next[0] = {...next[0], title: 'Changed title'};

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileNoMerge));
    });
  });

  describe('all issues change', () => {
    bench('merge:true', () => {
      const base = makeTree(ISSUE_COUNT);
      const next = cloneTree(base);
      for (let i = 0; i < ISSUE_COUNT; i++) {
        next[i] = {...next[i], title: `Updated ${i}`};
      }

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileMerge));
    });

    bench('merge:false', () => {
      const base = makeTree(ISSUE_COUNT);
      const next = cloneTree(base);
      for (let i = 0; i < ISSUE_COUNT; i++) {
        next[i] = {...next[i], title: `Updated ${i}`};
      }

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileNoMerge));
    });
  });
});

describe('reconcile merge:true vs merge:false (2000 issues)', () => {
  describe('single field edit', () => {
    bench('merge:true', () => {
      const base = makeTree(LARGE_COUNT);
      const next = cloneTree(base);
      next[0] = {...next[0], title: 'Changed title'};

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileMerge));
    });

    bench('merge:false', () => {
      const base = makeTree(LARGE_COUNT);
      const next = cloneTree(base);
      next[0] = {...next[0], title: 'Changed title'};

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileNoMerge));
    });
  });

  describe('all issues change', () => {
    bench('merge:true', () => {
      const base = makeTree(LARGE_COUNT);
      const next = cloneTree(base);
      for (let i = 0; i < LARGE_COUNT; i++) {
        next[i] = {...next[i], title: `Updated ${i}`};
      }

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileMerge));
    });

    bench('merge:false', () => {
      const base = makeTree(LARGE_COUNT);
      const next = cloneTree(base);
      for (let i = 0; i < LARGE_COUNT; i++) {
        next[i] = {...next[i], title: `Updated ${i}`};
      }

      const [, setState] = createStore<{issues: Issue[]}>({
        issues: cloneTree(base),
      });
      setState('issues', reconcile(next, reconcileNoMerge));
    });
  });
});
