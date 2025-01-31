# Interface: LogSink

## Methods

### flush()?

> `optional` **flush**(): `Promise`\<`void`\>

#### Returns

`Promise`\<`void`\>

***

### log()

> **log**(`level`, `context`, ...`args`): `void`

#### Parameters

##### level

[`LogLevel`](../type-aliases/LogLevel.md)

##### context

`undefined` | `Context`

##### args

...`unknown`[]

#### Returns

`void`
