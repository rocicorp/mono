import {expect, test} from 'vitest';
import {IVMSourceBranch} from './ivm-source-repo.ts';

test('fork', () => {
  const main = new IVMSourceBranch({
    users: {
      name: 'users',
      columns: {
        id: {type: 'string'},
        name: {type: 'string'},
      },
      primaryKey: ['id'],
    },
  });
  const mainConnection = main.getSource('users')!.connect([['id', 'asc']]);

  // Add initial data to main
  const usersSource = main.getSource('users')!;
  usersSource.push({
    type: 'add',
    row: {id: 'u1', name: 'Alice'},
  });

  // Fork should have same initial data
  const fork = main.fork();
  const forkConnection = fork.getSource('users')!.connect([['id', 'asc']]);
  expect([...forkConnection.fetch({})]).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "id": "u1",
          "name": "Alice",
        },
      },
    ]
  `);

  // Mutate main
  usersSource.push({
    type: 'add',
    row: {id: 'u2', name: 'Bob'},
  });

  // Mutate fork differently
  fork.getSource('users')!.push({
    type: 'add',
    row: {id: 'u3', name: 'Charlie'},
  });

  // Verify main and fork evolved independently
  expect([...mainConnection.fetch({})]).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "id": "u1",
          "name": "Alice",
        },
      },
      {
        "relationships": {},
        "row": {
          "id": "u2",
          "name": "Bob",
        },
      },
    ]
  `);

  expect([...forkConnection.fetch({})]).toMatchInlineSnapshot(`
    [
      {
        "relationships": {},
        "row": {
          "id": "u1",
          "name": "Alice",
        },
      },
      {
        "relationships": {},
        "row": {
          "id": "u3",
          "name": "Charlie",
        },
      },
    ]
  `);
});
