---
'zero-swift': patch
---

JSON is parsed, written, compared, and hashed (also as a custom query's arguments) without recursion, so deeply nested JSON no longer overflows the stack. Before, a debug build overflowed at about 300 levels on a thread with a 512 KB stack, such as a test or cooperative thread on macOS. The parser's depth limit is lowered from 5,000 to 1,500 levels, so freeing a value, which still recurses, fits in a 512 KB stack in a release build. JS has no such limit: a row whose JSON column is nested deeper than about 1,495 levels is rejected, as described in JSParity.md. A debug build can still overflow a 512 KB stack when it frees objects nested more than about 1,000 deep.
