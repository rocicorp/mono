# Type Alias: Pusher()

> **Pusher**: (`requestBody`, `requestID`) => `Promise`\<[`PusherResult`](PusherResult.md)\>

Pusher is the function type used to do the fetch part of a push. The request
is a POST request where the body is JSON with the type [PushRequest](PushRequest.md).

The return value should either be a [HTTPRequestInfo](HTTPRequestInfo.md) or a
[PusherResult](PusherResult.md). The reason for the two different return types is that
we didn't use to care about the response body of the push request. The
default pusher implementation checks if the response body is JSON and if it
matches the type PusherResponse. If it does, it is included in the
return value.

## Parameters

### requestBody

[`PushRequest`](PushRequest.md)

### requestID

`string`

## Returns

`Promise`\<[`PusherResult`](PusherResult.md)\>
