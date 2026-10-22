---
'zero-swift': patch
---

A first poke that changes nothing after a reconnect or relaunch completes the got queries, as in JS. `<=` and `>=` filters no longer match `NaN`. Storage: every client sharing a database hears about a write failure, and a closed client doesn't; writes queued before a failure no longer land after it; a database busy past the busy timeout is retried by the next write instead of being a storage failure; a read failure loading the client group is reported through `onStorageFailure` and moves the client to memory; and the memory fallback after a failed open is shared by name in the process, like `.memory`.
