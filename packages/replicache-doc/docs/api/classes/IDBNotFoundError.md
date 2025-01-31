# Class: IDBNotFoundError

This Error is thrown when we detect that the IndexedDB has been
removed. This does not normally happen but can happen during development if
the user has DevTools open and deletes the IndexedDB from there.

## Extends

- `Error`

## Constructors

### new IDBNotFoundError()

> **new IDBNotFoundError**(`message`?): [`IDBNotFoundError`](IDBNotFoundError.md)

#### Parameters

##### message?

`string`

#### Returns

[`IDBNotFoundError`](IDBNotFoundError.md)

#### Inherited from

`Error.constructor`

### new IDBNotFoundError()

> **new IDBNotFoundError**(`message`?, `options`?): [`IDBNotFoundError`](IDBNotFoundError.md)

#### Parameters

##### message?

`string`

##### options?

`ErrorOptions`

#### Returns

[`IDBNotFoundError`](IDBNotFoundError.md)

#### Inherited from

`Error.constructor`

## Properties

### cause?

> `optional` **cause**: `unknown`

#### Inherited from

`Error.cause`

***

### message

> **message**: `string`

#### Inherited from

`Error.message`

***

### name

> **name**: `string` = `'IDBNotFoundError'`

#### Overrides

`Error.name`

***

### stack?

> `optional` **stack**: `string`

#### Inherited from

`Error.stack`

***

### prepareStackTrace()?

> `static` `optional` **prepareStackTrace**: (`err`, `stackTraces`) => `any`

Optional override for formatting stack traces

#### Parameters

##### err

`Error`

##### stackTraces

`CallSite`[]

#### Returns

`any`

#### See

https://v8.dev/docs/stack-trace-api#customizing-stack-traces

#### Inherited from

`Error.prepareStackTrace`

***

### stackTraceLimit

> `static` **stackTraceLimit**: `number`

#### Inherited from

`Error.stackTraceLimit`

## Methods

### captureStackTrace()

> `static` **captureStackTrace**(`targetObject`, `constructorOpt`?): `void`

Create .stack property on a target object

#### Parameters

##### targetObject

`object`

##### constructorOpt?

`Function`

#### Returns

`void`

#### Inherited from

`Error.captureStackTrace`
