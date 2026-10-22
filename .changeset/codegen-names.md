---
'zero-swift': patch
---

`codegen` generates Swift that compiles for any table, column, relationship, JSON field, or enumeration value name. A name that is taken, a keyword Swift doesn't allow even backticked, a member the generated code declares, or a type it uses is renamed by a documented rule, and keeps its original name in the schema, `CodingKeys`, and raw values. Names outside ASCII are kept where Swift allows them, enumeration values with control characters are escaped as Swift allows, and a recursive `json<T>()` type is `JSONValue` where it recurs instead of overflowing the stack.
