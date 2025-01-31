# Interface: ReplicacheOptions\<MD\>

The options passed to [Replicache](../classes/Replicache.md).

## Type Parameters

• **MD** *extends* [`MutatorDefs`](../type-aliases/MutatorDefs.md)

## Properties

### auth?

> `optional` **auth**: `string`

This is the authorization token used when doing a
[pull](https://doc.replicache.dev/reference/server-pull#authorization) and
[push](https://doc.replicache.dev/reference/server-push#authorization).

***

### clientMaxAgeMs?

> `optional` **clientMaxAgeMs**: `number`

The maximum age of a client in milliseconds. If a client hasn't been seen
and has no pending mutations for this long, it will be removed from the
cache. Default is 24 hours.

This means that this is the maximum time a tab can be in the background
(frozen or in fbcache) and still be able to sync when it comes back to the
foreground. If tab comes back after this time the
onClientStateNotFound callback is called on the Replicache
instance.

***

### indexes?

> `readonly` `optional` **indexes**: [`IndexDefinitions`](../type-aliases/IndexDefinitions.md)

Defines the indexes, if any, to use on the data.

***

### kvStore?

> `optional` **kvStore**: [`KVStoreProvider`](../type-aliases/KVStoreProvider.md) \| `"mem"` \| `"idb"`

Allows providing a custom implementation of the underlying storage layer.

***

### ~~licenseKey?~~

> `optional` **licenseKey**: `string`

#### Deprecated

Replicache no longer uses a license key. This option is now
ignored and will be removed in a future release.

***

### logLevel?

> `optional` **logLevel**: [`LogLevel`](../type-aliases/LogLevel.md)

Determines how much logging to do. When this is set to `'debug'`,
Replicache will also log `'info'` and `'error'` messages. When set to
`'info'` we log `'info'` and `'error'` but not `'debug'`. When set to
`'error'` we only log `'error'` messages.
Default is `'info'`.

***

### logSinks?

> `optional` **logSinks**: [`LogSink`](LogSink.md)[]

Enables custom handling of logs.

By default logs are logged to the console.  If you would like logs to be
sent elsewhere (e.g. to a cloud logging service like DataDog) you can
provide an array of [LogSink](LogSink.md)s.  Logs at or above
[ReplicacheOptions.logLevel](ReplicacheOptions.md#loglevel) are sent to each of these [LogSink](LogSink.md)s.
If you would still like logs to go to the console, include
`consoleLogSink` in the array.

```ts
logSinks: [consoleLogSink, myCloudLogSink],
```

***

### mutators?

> `optional` **mutators**: `MD`

An object used as a map to define the *mutators*. These gets registered at
startup of [Replicache](../classes/Replicache.md).

*Mutators* are used to make changes to the data.

#### Example

The registered *mutations* are reflected on the
[mutate](../classes/Replicache.md#mutate) property of the [Replicache](../classes/Replicache.md) instance.

```ts
const rep = new Replicache({
  name: 'user-id',
  mutators: {
    async createTodo(tx: WriteTransaction, args: JSONValue) {
      const key = `/todo/${args.id}`;
      if (await tx.has(key)) {
        throw new Error('Todo already exists');
      }
      await tx.set(key, args);
    },
    async deleteTodo(tx: WriteTransaction, id: number) {
      ...
    },
  },
});
```

This will create the function to later use:

```ts
await rep.mutate.createTodo({
  id: 1234,
  title: 'Make things work offline',
  complete: true,
});
```

#### Replays

*Mutators* run once when they are initially invoked, but they might also be
*replayed* multiple times during sync. As such *mutators* should not modify
application state directly. Also, it is important that the set of
registered mutator names only grows over time. If Replicache syncs and
needed *mutator* is not registered, it will substitute a no-op mutator, but
this might be a poor user experience.

#### Server application

During push, a description of each mutation is sent to the server's [push
endpoint](https://doc.replicache.dev/reference/server-push) where it is applied. Once
the *mutation* has been applied successfully, as indicated by the client
view's
[`lastMutationId`](https://doc.replicache.dev/reference/server-pull#lastmutationid)
field, the local version of the *mutation* is removed. See the [design
doc](https://doc.replicache.dev/design#commits) for additional details on
the sync protocol.

#### Transactionality

*Mutators* are atomic: all their changes are applied together, or none are.
Throwing an exception aborts the transaction. Otherwise, it is committed.
As with query and subscribe all reads will see a consistent view of
the cache while they run.

***

### name

> **name**: `string`

The name of the Replicache database.

It is important to use user specific names so that if there are multiple
tabs open for different distinct users their data is kept separate.

For efficiency and performance, a new [Replicache](../classes/Replicache.md) instance will
initialize its state from the persisted state of an existing [Replicache](../classes/Replicache.md)
instance with the same `name`, domain and browser profile.

Mutations from one [Replicache](../classes/Replicache.md) instance may be pushed using the
[ReplicacheOptions.auth](ReplicacheOptions.md#auth), [ReplicacheOptions.pushURL](ReplicacheOptions.md#pushurl),
[ReplicacheOptions.pullURL](ReplicacheOptions.md#pullurl), [ReplicacheOptions.pusher](ReplicacheOptions.md#pusher), and
[ReplicacheOptions.puller](ReplicacheOptions.md#puller)  of another Replicache instance with the same
`name`, domain and browser profile.

You can use multiple Replicache instances for the same user as long as the
names are unique.  e.g. `name: `$userID:$roomID`

***

### puller?

> `optional` **puller**: [`Puller`](../type-aliases/Puller.md)

Allows passing in a custom implementation of a [Puller](../type-aliases/Puller.md) function. This
function is called when doing a pull and it is responsible for
communicating with the server.

Normally, this is just a POST to a URL with a JSON body but you can provide
your own function if you need to do things differently.

***

### pullInterval?

> `optional` **pullInterval**: `null` \| `number`

The duration between each pull in milliseconds. Set this to `null` to
prevent pulling in the background.  Defaults to 60 seconds.

***

### pullURL?

> `optional` **pullURL**: `string`

This is the URL to the server endpoint dealing with pull. See [Pull
Endpoint Reference](https://doc.replicache.dev/reference/server-pull) for more
details.

If not provided, pull requests will not be made unless a custom
[ReplicacheOptions.puller](ReplicacheOptions.md#puller) is provided.

***

### pushDelay?

> `optional` **pushDelay**: `number`

The delay between when a change is made to Replicache and when Replicache
attempts to push that change.

***

### pusher?

> `optional` **pusher**: [`Pusher`](../type-aliases/Pusher.md)

Allows passing in a custom implementation of a [Pusher](../type-aliases/Pusher.md) function. This
function is called when doing a push and it is responsible for
communicating with the server.

Normally, this is just a POST to a URL with a JSON body but you can provide
your own function if you need to do things differently.

***

### pushURL?

> `optional` **pushURL**: `string`

This is the URL to the server endpoint dealing with the push updates. See
[Push Endpoint Reference](https://doc.replicache.dev/reference/server-push) for more
details.

If not provided, push requests will not be made unless a custom
[ReplicacheOptions.pusher](ReplicacheOptions.md#pusher) is provided.

***

### requestOptions?

> `optional` **requestOptions**: [`RequestOptions`](RequestOptions.md)

Options to use when doing pull and push requests.

***

### schemaVersion?

> `optional` **schemaVersion**: `string`

The schema version of the data understood by this application. This enables
versioning of mutators (in the push direction) and the client view (in the
pull direction).
