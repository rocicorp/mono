# Interface: RequestOptions

## Properties

### maxDelayMs?

> `optional` **maxDelayMs**: `number`

When there are pending pull or push requests this is the _maximum_ amount
of time to wait until we try another pull/push.

***

### minDelayMs?

> `optional` **minDelayMs**: `number`

When there are pending pull or push requests this is the _minimum_ amount
of time to wait until we try another pull/push.
