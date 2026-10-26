---
'zero-swift': patch
---

Add `Zero.mutationOutcomes` and `Zero.addMutationOutcomeListener(_:)`, which report the server's outcome of every mutation once, including mutations replayed after a disconnect, whose `server` already settled with `.zeroError`.
