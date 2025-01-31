# Type Alias: Puller()

> **Puller**: (`requestBody`, `requestID`) => `Promise`\<[`PullerResult`](PullerResult.md)\>

Puller is the function type used to do the fetch part of a pull.

Puller needs to support dealing with pull request of version 0 and 1. Version
0 is used when doing mutation recovery of old clients. If a
PullRequestV1 is passed in the n a PullerResultV1 should
be returned. We do a runtime assert to make this is the case.

If you do not support old clients you can just throw if `pullVersion` is `0`,

## Parameters

### requestBody

[`PullRequest`](PullRequest.md)

### requestID

`string`

## Returns

`Promise`\<[`PullerResult`](PullerResult.md)\>
