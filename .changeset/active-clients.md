---
'zero-swift': patch
---

Two `Zero` instances of one client group in one process (the same user, `storageKey`, and storage, such as two windows) no longer drop each other's queries: `initConnection` lists every running client of the group in `activeClients`, as in JS, so zero-cache doesn't delete the other one. When one closes, the others send `deleteClients` for it once.
