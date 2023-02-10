---
title: First Replicache Feature
slug: /first-replicache-feature
---

# My First Replicache Feature

Let's see how easy it is to add a full-stack feature using Replicache. We will add an "urgent" flag to our Todos, and the ability to toggle and persist this property.

## Modify the Model

First let's add the `urgent` boolean to the Todo model.

:::tip

Because there is already data stored on the client and server that doesn't have this field, we can't mark it required. In a real application you could also use the [`schemaVersion`](https://doc.replicache.dev/server-pull#schemaversion) feature to migrate the old data, which would allow you to make the new field required.

:::

```ts title="src/todo.ts"
import {ReadTransaction} from 'replicache';

export type Todo = {
  id: string;
  text: string;
  completed: boolean;
  sort: number;

  // Add this property.
  urgent?: boolean;
};

export type TodoUpdate = Partial<Todo> & Pick<Todo, 'id'>;

export async function listTodos(tx: ReadTransaction) {
  return (await tx.scan().values().toArray()) as Todo[];
}
```

## Add a Mutator

We need a mutator to save the new field.

The example app already has an `updateTodo` mutator that handles all fields of `Todo`, so there's nothing to do here 🎉:

```ts title="src/mutators.ts"
export const mutators = {
  // This already stores all fields of `Todo`. Whee.
  updateTodo: async (tx: WriteTransaction, update: TodoUpdate) => {
    const prev = (await tx.get(update.id)) as Todo;
    const next = { ...prev, ...update };
    await tx.put(next.id, next);
  },
  ...
}
```

:::info

It's common for Replicache apps to have basic CRUD-style mutators for each entity they support. But it's also possible to create more complex mutators for more interesting situations. See the `createTodo` mutator in [mutators.ts](https://github.com/rocicorp/replicache-todo/blob/main/src/mutators.ts#L46) for an example.

:::

## Add a Toggle Button

We need to add a UI element so that the user can toggle the "urgent" flag. This is simple to do since the mutator we need is already available in this component as `onUpdate`.

```tsx title="src/components/todo-item.tsx"
<div className="view">
  ...
  {/* add this button to the view div right before the "destroy" button */}
  <button
    style={{
      position: 'absolute',
      top: 0,
      bottom: 0,
      right: 50,
      width: 40,
      height: 40,
      fontSize: 30,
      margin: 'auto 0',
    }}
    onClick={() => onUpdate({id, urgent: !todo.urgent})}
  >
    ❕
  </button>
  <button className="destroy" onClick={() => onDelete()} />
</div>
```

At this point, we have actually finished the basic plumbing of our feature. Clicking on this button will: 1) change the state of our app (immediately), 2) persist that change, and 3) cause that change to be synchronized _in real time_ to other browsers.

## Show the "urgent" flag in the UI

Just to prove to ourselves that this is happening, let's change the look of the todo when it's urgent:

```tsx title="src/components/todo-item.tsx"
{/* add the style attribute to change the todo-item's background when urgent */}
<div
  className="view"
  style={{
    backgroundColor: todo.urgent ? "red" : "",
  }}
>
  <input
    className="toggle"
    type="checkbox"
    checked={todo.completed}
    onChange={handleToggleComplete}
  />
  ...
```

It's not beautiful, but you get the idea. In summary, developers can often implement a feature by writing relatively simple code in one place. The data changes associated with that feature will automatically be full-stack and synchronized to other instances of the app.

# Next

Now let's [Deploy our app to production](/deploy-render).
