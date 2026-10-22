---
'zero-swift': minor
---

Depending on zero-swift no longer fetches swift-nio, swift-atomics, swift-collections, and swift-system. The `ZeroNIOTesting` product is removed: its `NIOTransport`, a plain `ws://` transport for Linux, is now part of zero-swift's own end-to-end tests.
