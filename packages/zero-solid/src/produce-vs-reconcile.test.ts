import {createEffect} from 'solid-js';
import {createStore, produce, reconcile} from 'solid-js/store';
import {describe, expect, test} from 'vitest';

// A symbol used as a key for reconcile to match objects by identity.
const ID_SYM = Symbol('id');

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

function cloneTree(tree: Issue[]): Issue[] {
  return tree.map(issue => ({
    ...issue,
    labels: issue.labels.map(l => ({...l})),
    creator: {...issue.creator},
    assignee: {...issue.assignee},
  }));
}

const reconcileOpts = {key: ID_SYM as unknown as string, merge: true};

describe('notification granularity', () => {
  test('produce: edit one field fires only that field effect', () => {
    const fired: string[] = [];

    const [state, setState] = createStore<{issues: Issue[]}>({
      issues: cloneTree(makeTree(3)),
    });

    createEffect(() => {
      void state.issues[0].title;
      fired.push('issue0.title');
    });
    createEffect(() => {
      void state.issues[1].title;
      fired.push('issue1.title');
    });
    createEffect(() => {
      void state.issues[0].description;
      fired.push('issue0.description');
    });

    // Clear initial fires
    fired.length = 0;

    setState(
      produce(draft => {
        draft.issues[0].title = 'Changed';
      }),
    );

    expect(fired).toEqual(['issue0.title']);
  });

  test('reconcile: edit one field fires only that field effect', () => {
    const fired: string[] = [];

    const base = makeTree(3);
    const [state, setState] = createStore<{issues: Issue[]}>({
      issues: cloneTree(base),
    });

    createEffect(() => {
      void state.issues[0].title;
      fired.push('issue0.title');
    });
    createEffect(() => {
      void state.issues[1].title;
      fired.push('issue1.title');
    });
    createEffect(() => {
      void state.issues[0].description;
      fired.push('issue0.description');
    });

    fired.length = 0;

    const next = cloneTree(base);
    next[0] = {...next[0], title: 'Changed'};
    setState('issues', reconcile(next, reconcileOpts));

    expect(fired).toEqual(['issue0.title']);
  });

  test('reconcile: edit nested child fires only that child effect', () => {
    const fired: string[] = [];

    const base = makeTree(3);
    const [state, setState] = createStore<{issues: Issue[]}>({
      issues: cloneTree(base),
    });

    createEffect(() => {
      void state.issues[0].labels[0].name;
      fired.push('issue0.label0.name');
    });
    createEffect(() => {
      void state.issues[0].labels[1].name;
      fired.push('issue0.label1.name');
    });
    createEffect(() => {
      void state.issues[1].labels[0].name;
      fired.push('issue1.label0.name');
    });
    createEffect(() => {
      void state.issues[0].assignee.name;
      fired.push('issue0.assignee.name');
    });

    fired.length = 0;

    const next = cloneTree(base);
    next[0] = {
      ...next[0],
      labels: next[0].labels.map((l, i) =>
        i === 1 ? {...l, name: 'Updated'} : l,
      ),
    };
    setState('issues', reconcile(next, reconcileOpts));

    expect(fired).toEqual(['issue0.label1.name']);
  });

  test('reconcile: add row fires array length effect, not unrelated row effects', () => {
    const fired: string[] = [];

    const base = makeTree(3);
    const [state, setState] = createStore<{issues: Issue[]}>({
      issues: cloneTree(base),
    });

    createEffect(() => {
      void state.issues.length;
      fired.push('issues.length');
    });
    createEffect(() => {
      void state.issues[0].title;
      fired.push('issue0.title');
    });
    createEffect(() => {
      void state.issues[1].title;
      fired.push('issue1.title');
    });

    fired.length = 0;

    const next = [...cloneTree(base), makeIssue(4)];
    setState('issues', reconcile(next, reconcileOpts));

    expect(fired).toContain('issues.length');
    expect(fired).not.toContain('issue0.title');
    expect(fired).not.toContain('issue1.title');
  });

  test('reconcile: remove row fires array length effect, not unrelated row effects', () => {
    const fired: string[] = [];

    const base = makeTree(3);
    const [state, setState] = createStore<{issues: Issue[]}>({
      issues: cloneTree(base),
    });

    createEffect(() => {
      void state.issues.length;
      fired.push('issues.length');
    });
    createEffect(() => {
      void state.issues[0].title;
      fired.push('issue0.title');
    });

    fired.length = 0;

    // Remove the last issue so issue[0] is untouched
    const next = cloneTree(base).slice(0, 2);
    setState('issues', reconcile(next, reconcileOpts));

    expect(fired).toContain('issues.length');
    expect(fired).not.toContain('issue0.title');
  });
});
