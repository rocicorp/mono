# Replicache on Rails

Generates a CRUD-style interface for Replicache and Reflect, with optional schema validation.

## Install

```bash
npm install --save-dev @rocicorp/rails
```

## Usage

### 1. Define Entities

```ts
// All entities must include at least id:string.
export type Todo = {
  id: string;
  text: string;
  complete: boolean;
  sort: number;
};
```

### 2. Generate Helpers

```ts
import {generate} from '@rocicorp/rails';

export const {
  put: putTodo,
  get: getTodo,
  list: listTodos,
  // ...
} = generate<Todo>('todo');
```

### 3. Build Mutators

The generated functions all have the same signatures as mutators, so they can be used as mutators directly:

```ts
import {putTodo, updateTodo, deleteTodo} from './todo';

export const mutators = {
  putTodo,
  updateTodo,
  deleteTodo,
};
```

You can also compose them to make more advanced mutators:

```ts
import {listTodos, updateTodo} from './todo';

async function markAllComplete(tx: WriteTransaction) {
  const todos = await listTodos(tx);
  for (const t of todos) {
    // Mutators are transactional, so this is safe. The entire function will
    // run atomically and `t.complete` cannot change while it is running.
    if (!t.complete) {
      await updateTodo(todo.id, {complete: true});
    }
  }
}

export const mutators = {
  // ...
  markAllComplete,
};
```

### 4. Build Subscriptions

The generated functions that are read-only (`get`, `has`, `list`, etc) have the correct signature to be used as subscriptions. So you can use them directly:

```ts
// subscribe to a query
const todos = useSubscribe(rep, listTodos, []);
```

But as with mutators, you can also compose them to make more interesting subscriptions:

```ts
async function listIncompleteTodos(tx: WriteTransaction) {
  const todos = await listTodos(tx);
  return todos.filter(t => !t.complete);
}

const incompleteTodos = useSubscribe(rep, listIncompleteTodos, []);
```

## Validation

You can optionally pass `generate` a validation function as a second parameter. For example, to use Zod as your schema validator:

```ts
import * as z from 'zod';
import {generate} from '@rocicorp/rails';

const todoSchema = {
  id: z.string(),
  text: z.string(),
  complete: z.boolean(),
  sort: z.number(),
};

// In this case, the template parameter to generate can be omitted because it
// is inferred from return type of todoSchema.parse().
export const {
  put: putTodo,
  get: getTodo,
  update: updateTodo,
  delete: deleteTodo,
  list: listTodos,
} = generate('todo', todoSchema.parse);
```

## Reference

### `init(tx: WriteTransaction, value: T): Promise<boolean>`

Writes `value` if it is not already present. If `value` is already present, does nothing. Returns `true` if the value was written or false otherwise.

### `put(tx: WriteTransaction, value: T): Promise<void>`

Deprecated. Use `set` instead.

Writes `value`. If not present, creates, otherwise overwrites.

### `set(tx: WriteTransaction, value: T): Promise<void>`

Writes `value`. If not present, creates, otherwise overwrites.

### `update(tx: WriteTransaction, value: Update<Entity, T>) => Promise<void>`

Updates `value`. Value can specify any of the fields of `T` and must contain `id`. Fields not included in `value` are left unaffected.

All fields in an update are applied together atomically. If the entity does not exist a debug message is printed to the console and the update is skipped.

### `delete(tx: WriteTransaction, id: string) => Promise<void>`

Delete any existing value or do nothing if none exist.

### `has(tx: ReadTransaction, id: string) => Promise<boolean>`

Return true if specified value exists, false otherwise.

### `get(tx: ReadTransaction, id: string) => Promise<T | undefined>`

Get value by ID, or return undefined if none exists.

### `mustGet(tx: ReadTransaction, id: string) => Promise<T>`

Get value by ID, or throw if none exists.

### `list(tx: ReadTransaction, options?: {startAtID?: string, limit:? number}) => Promise<T[]>`

List values matching criteria.

### `listIDs(tx: ReadTransaction, options?: {startAtID?: string, limit:? number}) => Promise<string[]>`

List ids matching criteria.

### `listEntries(tx: ReadTransaction, options?: {startAtID?: string, limit:? number}) => Promise<[string, T][]>`

List `[id, value]` entries matching criteria.

## Presence State

Presence state is a special kind of state that is tied to a certain `clientID`.
It is designed to be used with Reflect's
[presence](https://hello.reflect.net/concepts/presence) feature.

The entity type for presence state is slightly different from the `Entity` type:

```ts
type PresenceEntity = {
  clientID: string;
  id: string;
};
```

### Generate Presence State Helpers

The function `generatePresence` is similar to the `generate` function but it
generates functions that are to be used with presence state.

```ts
type Cursor {
  clientID: string;
  x: number;
  y: number;
};

const {
  set: setCursor,
  get: getCursor,
  delete: deleteCursor,
} = generatePresence<Cursor>('cursor');
```

For presence entities there are two common cases:

1. The entity does not have an `id` field. Then there can only be one entity per
   client. This case is useful for keeping track of things like the cursor
   position.
2. The entity has an `id` field. Then there can be multiple entities per client.
   This case is useful for keeping track of things like multiple selection or
   multiple cursors (aka multi touch).

### Lookup Presence State

The `clientID` field (and `id` if used) is significant when reading presence
state. However, for convenience, you can omit the `clientID` and it will default
to the `clientID` of current client. You may not omit the `id` if your entity
type requires an `id` field. When reading presence state you may also omit the
lookup argument completely.

### Mutating Presence State

When writing you may only change the presence state entities for the current
client. If you pass in a `clientID` that is different from the `clientID` of the
current client a runtime error will be thrown.

When writing you may also omit the `clientID` it will default to the `clientID`
of the current client.

```ts
await setCursor(tx, {x: 10, y: 20});
expect(await getCursor(tx)).toEqual({
  clientID: tx.clientID,
  x: 10,
  y: 20,
});
```

## Reference for Presence State

### `set: (tx: WriteTransaction, value: OptionalClientID<T>) => Promise<void>`

Write `value`, overwriting any previous version of same value.

### `init: (tx: WriteTransaction, value: OptionalClientID<T>) => Promise<boolean>`

Write `value` only if no previous version of this value exists.

### `update: (tx: WriteTransaction, value: PresenceUpdate<T>) => Promise<void>`

Update existing value with new fields.

### `delete: (tx: WriteTransaction, id?: PresenceID<T>) => Promise<void>`

Delete any existing value or do nothing if none exist.

### `has: (tx: ReadTransaction, id?: PresenceID<T>) => Promise<boolean>`

Return true if specified value exists, false otherwise.

### `get: (tx: ReadTransaction, id?: PresenceID<T>) => Promise<T | undefined>`

Get value by ID, or return undefined if none exists.

### `mustGet: (tx: ReadTransaction, id?: PresenceID<T>) => Promise<T>`

Get value by ID, or throw if none exists.

### `list: (tx: ReadTransaction, options?: PresenceListOptions<T>) => Promise<T[]>`

List values matching criteria.

### `listIDs: (tx: ReadTransaction, options?: PresenceListOptions<T>) => Promise<ListID<T>[]>`

List IDs matching criteria. The returned ID is `{clientID: string}` if the entry
has no `id` field, otherwise it is `{clientID: string, id: string}`.

### `listClientIDs: (tx: ReadTransaction, options?: PresenceListOptions<T>) => Promise<string[]>`

List `clientID`s matching criteria. Unlike `listIDs` this returns an array of
strings consisting of the `clientID`s.

### `listEntries: (tx: ReadTransaction, options?: PresenceListOptions<T>) => Promise<[ListID<T>, T][]>`

List [id, value] entries matching criteria.

## Upgrade from 0.6

### Pluggable Schema

Rails 0.7 made the schema validator pluggable. Instead of passing an instance of zod, pass the parse function.

Before:

```ts
export const {
  put: putTodo,
  // ...
} = generate<Todo>('todo', todoSchema);
```

Now:

```ts
export const {
  put: putTodo,
  // ...
} = generate<Todo>('todo', todoSchema.parse);
```

### EntitySchema no longer part of Rails

Because the validator is pluggable it no longer makes sense for Rails to provide `entitySchema`. So either define it yourself:

```ts
const entitySchema = z.object({
  id: z.string(),
});
```

... or simply add `id: z.string()` to each of your entity definitions.

### Parse called in Debug mode

In 0.6.0, zod was only used if `process.env.NODE_ENV !== 'production'`. Now that the validator is pluggable, it makes more sense for the app to do this.

Before:

```ts
export const {
  put: putTodo,
  // ...
} = generate<Todo>('todo', todoSchema);
```

Now:

```ts
export const {
  put: putTodo,
  // ...
} = generate<Todo>(
  'todo',
  process.env.NODE_ENV !== 'production' ? todoSchema.parse : undefined,
);
```
