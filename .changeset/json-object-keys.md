---
'zero-swift': minor
---

`JSONValue.object` holds a `JSONObject` instead of a `[String: JSONValue]`. A `JSONObject` keeps its keys as a JS object does: integer-like keys first in ascending order, then the others in the order they were set, so JSON is written as `JSON.stringify` writes it. Keys are compared by UTF-16 code unit, so keys that differ only in Unicode normalization (`"é"` and `"e\u{301}"`) stay apart instead of one overwriting the other. Before, they merged in pokes (row values, primary keys, merges, client IDs), and custom-query and AST hashes of such keys differed from JS. Dictionary literals, `object[key]`, and `for (key, value) in object` work as before, and `Row(_:)` and `Row.object` convert both ways.

Migration: where code builds `.object(dict)` from a `[String: JSONValue]` variable, write `.object(JSONObject(dict))` or declare the variable as `JSONObject`. `objectValue` and `ZeroObjectReader.object`/`ZeroObjectWriter.object` are now `JSONObject`; use `.dictionary` where a `[String: JSONValue]` is needed. `AST.Bound.row` is a `JSONObject`, and `AnyQuery.start`, `TableMutator`, and `LoopbackServer.put` take a `JSONObject` as well as a dictionary.
