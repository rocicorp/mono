---
'zero-swift': minor
---

`zero.delete()` closes the client and deletes its local data: the replica and pending mutations of its user and `storageKey`, as in JS. `dropAllDatabases(_:)` deletes every Zero database in a storage location. Both return the `DroppedDatabases`.
