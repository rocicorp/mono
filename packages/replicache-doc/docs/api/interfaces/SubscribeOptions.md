# Interface: SubscribeOptions\<R\>

The options passed to [Replicache.subscribe](../classes/Replicache.md#subscribe).

## Type Parameters

• **R**

## Properties

### isEqual()?

> `optional` **isEqual**: (`a`, `b`) => `boolean`

If present this function is used to determine if the value returned by the
body function has changed. If not provided a JSON deep equality check is
used.

#### Parameters

##### a

`R`

##### b

`R`

#### Returns

`boolean`

***

### onData()

> **onData**: (`result`) => `void`

Called when the return value of the body function changes.

#### Parameters

##### result

`R`

#### Returns

`void`

***

### onDone()?

> `optional` **onDone**: () => `void`

If present, called when the subscription is removed/done.

#### Returns

`void`

***

### onError()?

> `optional` **onError**: (`error`) => `void`

If present, called when an error occurs.

#### Parameters

##### error

`unknown`

#### Returns

`void`
