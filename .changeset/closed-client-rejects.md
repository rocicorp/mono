---
'zero-swift': patch
---

On a closed `Zero`, a new `run` waiting for `.complete` or `.cached`, and a new preload's `cached` and `complete`, now throw the `QueryError` saying Zero was closed right away instead of never settling. Such a preload registers nothing, and a `run` with `.unknown` still returns the local rows. JS only rejects the calls pending at close, so this differs from JS.
