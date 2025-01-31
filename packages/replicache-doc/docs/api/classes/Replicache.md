# Class: Replicache\<MD\>

## Type Parameters

• **MD** *extends* [`MutatorDefs`](../type-aliases/MutatorDefs.md) = \{\}

## Constructors

### new Replicache()

> **new Replicache**\<`MD`\>(`options`): [`Replicache`](Replicache.md)\<`MD`\>

#### Parameters

##### options

[`ReplicacheOptions`](../interfaces/ReplicacheOptions.md)\<`MD`\>

#### Returns

[`Replicache`](Replicache.md)\<`MD`\>

## Accessors

### auth

#### Get Signature

> **get** **auth**(): `string`

The authorization token used when doing a push request.

##### Returns

`string`

#### Set Signature

> **set** **auth**(`value`): `void`

##### Parameters

###### value

`string`

##### Returns

`void`

***

### clientGroupID

#### Get Signature

> **get** **clientGroupID**(): `Promise`\<`string`\>

The client group ID for this instance of Replicache. Instances of
Replicache will have the same client group ID if and only if they have
the same name, mutators, indexes, schema version, format version, and
browser profile.

##### Returns

`Promise`\<`string`\>

***

### clientID

#### Get Signature

> **get** **clientID**(): `string`

The client ID for this instance of Replicache. Each instance of Replicache
gets a unique client ID.

##### Returns

`string`

***

### closed

#### Get Signature

> **get** **closed**(): `boolean`

Whether the Replicache database has been closed. Once Replicache has been
closed it no longer syncs and you can no longer read or write data out of
it. After it has been closed it is pretty much useless and should not be
used any more.

##### Returns

`boolean`

***

### getAuth

#### Get Signature

> **get** **getAuth**(): `undefined` \| `null` \| () => [`MaybePromise`](../type-aliases/MaybePromise.md)\<`undefined` \| `null` \| `string`\>

This gets called when we get an HTTP unauthorized (401) response from the
push or pull endpoint. Set this to a function that will ask your user to
reauthenticate.

##### Returns

`undefined` \| `null` \| () => [`MaybePromise`](../type-aliases/MaybePromise.md)\<`undefined` \| `null` \| `string`\>

#### Set Signature

> **set** **getAuth**(`value`): `void`

##### Parameters

###### value

`undefined` | `null` | () => [`MaybePromise`](../type-aliases/MaybePromise.md)\<`undefined` \| `null` \| `string`\>

##### Returns

`void`

***

### idbName

#### Get Signature

> **get** **idbName**(): `string`

This is the name Replicache uses for the IndexedDB database where data is
stored.

##### Returns

`string`

***

### mutate

#### Get Signature

> **get** **mutate**(): [`MakeMutators`](../type-aliases/MakeMutators.md)\<`MD`\>

The mutators that was registered in the constructor.

##### Returns

[`MakeMutators`](../type-aliases/MakeMutators.md)\<`MD`\>

***

### name

#### Get Signature

> **get** **name**(): `string`

The name of the Replicache database. Populated by [ReplicacheOptions#name](../interfaces/ReplicacheOptions.md#name).

##### Returns

`string`

***

### onClientStateNotFound

#### Get Signature

> **get** **onClientStateNotFound**(): `null` \| () => `void`

`onClientStateNotFound` is called when the persistent client has been
garbage collected. This can happen if the client has no pending mutations
and has not been used for a while.

The default behavior is to reload the page (using `location.reload()`). Set
this to `null` or provide your own function to prevent the page from
reloading automatically.

##### Returns

`null` \| () => `void`

#### Set Signature

> **set** **onClientStateNotFound**(`value`): `void`

##### Parameters

###### value

`null` | () => `void`

##### Returns

`void`

***

### online

#### Get Signature

> **get** **online**(): `boolean`

A rough heuristic for whether the client is currently online. Note that
there is no way to know for certain whether a client is online - the next
request can always fail. This property returns true if the last sync attempt succeeded,
and false otherwise.

##### Returns

`boolean`

***

### onOnlineChange

#### Get Signature

> **get** **onOnlineChange**(): `null` \| (`online`) => `void`

`onOnlineChange` is called when the [online](Replicache.md#online) property changes. See
[online](Replicache.md#online) for more details.

##### Returns

`null` \| (`online`) => `void`

#### Set Signature

> **set** **onOnlineChange**(`value`): `void`

##### Parameters

###### value

`null` | (`online`) => `void`

##### Returns

`void`

***

### onSync

#### Get Signature

> **get** **onSync**(): `null` \| (`syncing`) => `void`

`onSync(true)` is called when Replicache transitions from no push or pull
happening to at least one happening. `onSync(false)` is called in the
opposite case: when Replicache transitions from at least one push or pull
happening to none happening.

This can be used in a React like app by doing something like the following:

```js
const [syncing, setSyncing] = useState(false);
useEffect(() => {
  rep.onSync = setSyncing;
}, [rep]);
```

##### Returns

`null` \| (`syncing`) => `void`

#### Set Signature

> **set** **onSync**(`value`): `void`

##### Parameters

###### value

`null` | (`syncing`) => `void`

##### Returns

`void`

***

### onUpdateNeeded

#### Get Signature

> **get** **onUpdateNeeded**(): `null` \| (`reason`) => `void`

`onUpdateNeeded` is called when a code update is needed.

A code update can be needed because:
- the server no longer supports the pushVersion,
  pullVersion or [schemaVersion](Replicache.md#schemaversion) of the current code.
- a new Replicache client has created a new client group, because its code
  has different mutators, indexes, schema version and/or format version
  from this Replicache client. This is likely due to the new client having
  newer code. A code update is needed to be able to locally sync with this
  new Replicache client (i.e. to sync while offline, the clients can can
  still sync with each other via the server).

The default behavior is to reload the page (using `location.reload()`). Set
this to `null` or provide your own function to prevent the page from
reloading automatically. You may want to provide your own function to
display a toast to inform the end user there is a new version of your app
available and prompting them to refresh.

##### Returns

`null` \| (`reason`) => `void`

#### Set Signature

> **set** **onUpdateNeeded**(`value`): `void`

##### Parameters

###### value

`null` | (`reason`) => `void`

##### Returns

`void`

***

### profileID

#### Get Signature

> **get** **profileID**(): `Promise`\<`string`\>

The browser profile ID for this browser profile. Every instance of Replicache
browser-profile-wide shares the same profile ID.

##### Returns

`Promise`\<`string`\>

***

### puller

#### Get Signature

> **get** **puller**(): [`Puller`](../type-aliases/Puller.md)

The function to use to pull data from the server.

##### Returns

[`Puller`](../type-aliases/Puller.md)

#### Set Signature

> **set** **puller**(`value`): `void`

##### Parameters

###### value

[`Puller`](../type-aliases/Puller.md)

##### Returns

`void`

***

### pullInterval

#### Get Signature

> **get** **pullInterval**(): `null` \| `number`

The duration between each periodic [pull](Replicache.md#pull). Setting this to `null`
disables periodic pull completely. Pull will still happen if you call
[pull](Replicache.md#pull) manually.

##### Returns

`null` \| `number`

#### Set Signature

> **set** **pullInterval**(`value`): `void`

##### Parameters

###### value

`null` | `number`

##### Returns

`void`

***

### pullURL

#### Get Signature

> **get** **pullURL**(): `string`

The URL to use when doing a pull request.

##### Returns

`string`

#### Set Signature

> **set** **pullURL**(`value`): `void`

##### Parameters

###### value

`string`

##### Returns

`void`

***

### pushDelay

#### Get Signature

> **get** **pushDelay**(): `number`

The delay between when a change is made to Replicache and when Replicache
attempts to push that change.

##### Returns

`number`

#### Set Signature

> **set** **pushDelay**(`value`): `void`

##### Parameters

###### value

`number`

##### Returns

`void`

***

### pusher

#### Get Signature

> **get** **pusher**(): [`Pusher`](../type-aliases/Pusher.md)

The function to use to push data to the server.

##### Returns

[`Pusher`](../type-aliases/Pusher.md)

#### Set Signature

> **set** **pusher**(`value`): `void`

##### Parameters

###### value

[`Pusher`](../type-aliases/Pusher.md)

##### Returns

`void`

***

### pushURL

#### Get Signature

> **get** **pushURL**(): `string`

The URL to use when doing a push request.

##### Returns

`string`

#### Set Signature

> **set** **pushURL**(`value`): `void`

##### Parameters

###### value

`string`

##### Returns

`void`

***

### requestOptions

#### Get Signature

> **get** **requestOptions**(): `Required`\<[`RequestOptions`](../interfaces/RequestOptions.md)\>

The options used to control the [pull](Replicache.md#pull) and push request behavior. This
object is live so changes to it will affect the next pull or push call.

##### Returns

`Required`\<[`RequestOptions`](../interfaces/RequestOptions.md)\>

***

### schemaVersion

#### Get Signature

> **get** **schemaVersion**(): `string`

The schema version of the data understood by this application.

##### Returns

`string`

## Methods

### close()

> **close**(): `Promise`\<`void`\>

Closes this Replicache instance.

When closed all subscriptions end and no more read or writes are allowed.

#### Returns

`Promise`\<`void`\>

***

### experimentalPendingMutations()

> **experimentalPendingMutations**(): `Promise`\<readonly [`PendingMutation`](../type-aliases/PendingMutation.md)[]\>

**`Experimental`**

List of pending mutations. The order of this is from oldest to newest.

Gives a list of local mutations that have `mutationID` >
`syncHead.mutationID` that exists on the main client group.

 This method is experimental and may change in the future.

#### Returns

`Promise`\<readonly [`PendingMutation`](../type-aliases/PendingMutation.md)[]\>

***

### experimentalWatch()

#### Call Signature

> **experimentalWatch**(`callback`): () => `void`

**`Experimental`**

Watches Replicache for changes.

The `callback` gets called whenever the underlying data changes and the
`key` changes matches the `prefix` of [ExperimentalWatchIndexOptions](../type-aliases/ExperimentalWatchIndexOptions.md) or
[ExperimentalWatchNoIndexOptions](../type-aliases/ExperimentalWatchNoIndexOptions.md) if present. If a change
occurs to the data but the change does not impact the key space the
callback is not called. In other words, the callback is never called with
an empty diff.

This gets called after commit (a mutation or a rebase).

 This method is under development and its semantics will
change.

##### Parameters

###### callback

[`ExperimentalWatchNoIndexCallback`](../type-aliases/ExperimentalWatchNoIndexCallback.md)

##### Returns

`Function`

###### Returns

`void`

#### Call Signature

> **experimentalWatch**\<`Options`\>(`callback`, `options`?): () => `void`

**`Experimental`**

Watches Replicache for changes.

The `callback` gets called whenever the underlying data changes and the
`key` changes matches the `prefix` of [ExperimentalWatchIndexOptions](../type-aliases/ExperimentalWatchIndexOptions.md) or
[ExperimentalWatchNoIndexOptions](../type-aliases/ExperimentalWatchNoIndexOptions.md) if present. If a change
occurs to the data but the change does not impact the key space the
callback is not called. In other words, the callback is never called with
an empty diff.

This gets called after commit (a mutation or a rebase).

 This method is under development and its semantics will
change.

##### Type Parameters

• **Options** *extends* [`ExperimentalWatchOptions`](../type-aliases/ExperimentalWatchOptions.md)

##### Parameters

###### callback

[`ExperimentalWatchCallbackForOptions`](../type-aliases/ExperimentalWatchCallbackForOptions.md)\<`Options`\>

###### options?

`Options`

##### Returns

`Function`

###### Returns

`void`

***

### poke()

> **poke**(`poke`): `Promise`\<`void`\>

**`Experimental`**

Applies an update from the server to Replicache.
Throws an error if cookie does not match. In that case the server thinks
this client has a different cookie than it does; the caller should disconnect
from the server and re-register, which transmits the cookie the client actually
has.

 This method is under development and its semantics will change.

#### Parameters

##### poke

[`Poke`](../type-aliases/Poke.md)

#### Returns

`Promise`\<`void`\>

***

### pull()

> **pull**(`now`?): `Promise`\<`void`\>

Pull pulls changes from the [pullURL](Replicache.md#pullurl). If there are any changes local
changes will get replayed on top of the new server state.

If the server endpoint fails pull will be continuously retried with an
exponential backoff.

#### Parameters

##### now?

If true, pull will happen immediately and ignore
  [RequestOptions.minDelayMs](../interfaces/RequestOptions.md#mindelayms) as well as the exponential backoff in
  case of errors.

###### now

`boolean` = `false`

#### Returns

`Promise`\<`void`\>

A promise that resolves when the next pull completes. In case of
errors the first error will reject the returned promise. Subsequent errors
will not be reflected in the promise.

***

### push()

> **push**(`now`?): `Promise`\<`void`\>

Push pushes pending changes to the [pushURL](Replicache.md#pushurl).

You do not usually need to manually call push. If [pushDelay](Replicache.md#pushdelay) is
non-zero (which it is by default) pushes happen automatically shortly after
mutations.

If the server endpoint fails push will be continuously retried with an
exponential backoff.

#### Parameters

##### now?

If true, push will happen immediately and ignore
  [pushDelay](Replicache.md#pushdelay), [RequestOptions.minDelayMs](../interfaces/RequestOptions.md#mindelayms) as well as the
  exponential backoff in case of errors.

###### now

`boolean` = `false`

#### Returns

`Promise`\<`void`\>

A promise that resolves when the next push completes. In case of
errors the first error will reject the returned promise. Subsequent errors
will not be reflected in the promise.

***

### query()

> **query**\<`R`\>(`body`): `Promise`\<`R`\>

Query is used for read transactions. It is recommended to use transactions
to ensure you get a consistent view across multiple calls to `get`, `has`
and `scan`.

#### Type Parameters

• **R**

#### Parameters

##### body

(`tx`) => `R` \| `Promise`\<`R`\>

#### Returns

`Promise`\<`R`\>

***

### subscribe()

> **subscribe**\<`R`\>(`body`, `options`): () => `void`

Subscribe to the result of a [query](Replicache.md#query). The `body` function is
evaluated once and its results are returned via `onData`.

Thereafter, each time the the result of `body` changes, `onData` is fired
again with the new result.

`subscribe()` goes to significant effort to avoid extraneous work
re-evaluating subscriptions:

1. subscribe tracks the keys that `body` accesses each time it runs. `body`
   is only re-evaluated when those keys change.
2. subscribe only re-fires `onData` in the case that a result changes by
   way of the `isEqual` option which defaults to doing a deep JSON value
   equality check.

Because of (1), `body` must be a pure function of the data in Replicache.
`body` must not access anything other than the `tx` parameter passed to it.

Although subscribe is as efficient as it can be, it is somewhat constrained
by the goal of returning an arbitrary computation of the cache. For even
better performance (but worse dx), see [experimentalWatch](Replicache.md#experimentalwatch).

If an error occurs in the `body` the `onError` function is called if
present. Otherwise, the error is logged at log level 'error'.

To cancel the subscription, call the returned function.

#### Type Parameters

• **R**

#### Parameters

##### body

(`tx`) => `Promise`\<`R`\>

The function to evaluate to get the value to pass into
   `onData`.

##### options

Options is either a function or an object. If it is a
   function it is equivalent to passing it as the `onData` property of an
   object.

[`SubscribeOptions`](../interfaces/SubscribeOptions.md)\<`R`\> | (`result`) => `void`

#### Returns

`Function`

##### Returns

`void`
