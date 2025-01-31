# Class: PushError

This error is thrown when the pusher fails for any reason.

## Extends

- `Error`

## Constructors

### new PushError()

> **new PushError**(`causedBy`?): [`PushError`](PushError.md)

#### Parameters

##### causedBy?

`Error`

#### Returns

[`PushError`](PushError.md)

#### Overrides

`Error.constructor`

## Properties

### cause?

> `optional` **cause**: `unknown`

#### Inherited from

`Error.cause`

***

### causedBy?

> `optional` **causedBy**: `Error`

***

### message

> **message**: `string`

#### Inherited from

`Error.message`

***

### name

> **name**: `string` = `'PushError'`

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
