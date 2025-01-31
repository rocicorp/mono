---
title: Undo
slug: /howto/undo
---

Because of Replicache's [conflict resolution model](/concepts/how-it-works#conflict-resolution) and [undo manager](https://github.com/rocicorp/undo), adding undo to your Replicache app is easy. All of the complexities of multi player undo are handled by Replicache. This HOWTO will walk through the process of adding undo to our sample [todo app](https://github.com/rocicorp/replicache-todo).

### Add undo library

```
npm install @rocicorp/undo
```

### Instantiate UndoManager

```ts
const undoManager = new UndoManager();
```

### Use the undo manager to associate executed mutations with undo mutations

It is important to keep track of all the mutations you want to undo. For every mutation that you want to perform, you will need an inverse mutation for undo.
Instead of calling Replicache mutations directly you will want to wrap the calls using `undoManager.add`.

```tsx
  // new item with undo
  const handleNewItem = (text: string) => {
    const id = nanoid();
    undoManager.add({
      execute: () => {
        rep.mutate.putTodo({
          id,
          text: text,
          sort: todos.length > 0 ? todos[todos.length - 1].sort + 1 : 0,
          completed: false,
        });
      };,
      undo: () => rep.mutate.deleteTodos([id]),
    });
  };

```

### Calling `undo` and `redo` functions

It is as easy as calling `undoManager.undo` and `undoManager.redo` to undo or redo your mutations.

### Add keyboard bindings for `undo` / `redo` (optional)

Usually you want to bind keyboard events to undo / redo. (ctrl+z, cmd+z, command+shift+z, ctrl+shift+z). You can capture keyboard events or use a library like `react-hotkeys`.

```tsx
  const handlers = {
    undo: () => undoManager.undo(),
    redo: () => undoManager.redo(),
  };

  const keyMap = {
    undo: ["ctrl+z", "command+z"],
    redo: ["ctrl+y", "command+shift+z", "ctrl+shift+z"],
  };

  return (
    <Hotkeys
      {...{
        keyMap,
        handlers,
      }}
    >
      ...
    </HotKeys>
```

### Basic Example

```tsx
import { Replicache } from "replicache";
import { useSubscribe } from "replicache-react";
...
import { UndoManager } from "@rocicorp/undo";
import { HotKeys } from "react-hotkeys";

// Replicache and UndoManager are initialized outside of the initial component render.
// undoManager = new UndoManager()
const App = ({ rep }: { rep: Replicache<M>; undoManager: UndoManager }) => {
  const todos = useSubscribe(rep, listTodos, [], [rep]);

  // new item with undo
  const handleNewItem = (text: string) => {
      const id = nanoid();
      undoManager.add({
        execute: () => {
          rep.mutate.putTodo({
            id,
            text: text,
            sort: todos.length > 0 ? todos[todos.length - 1].sort + 1 : 0,
            completed: false,
          });
        };,
        undo: () => rep.mutate.deleteTodos([id]),
      });
    };
  };

  const handlers = {
    undo: () => undoManager.undo(),
    redo: () => undoManager.redo(),
  };

  const keyMap = {
    undo: ["ctrl+z", "command+z"],
    redo: ["ctrl+y", "command+shift+z", "ctrl+shift+z"],
  };

  return (
    <Hotkeys
      {...{
        keyMap,
        handlers,
      }}
    >
        <Header onNewItem={handleNewItem} />
        <MainSection todos={todos} />
    </Hotkeys>
  );
};
```

`Undo` library can handle more complex features like `grouping` and `onChange` events. You can look at full integrations of undo with the [Repliear](http://github.com/rocicorp/repliear) projects. Please reference the [Undo](http://github.com/rocicorp/undo) library for more information.
