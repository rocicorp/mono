/// A change to the synced rows (`rowPatchOpSchema` in
/// `packages/zero-protocol/src/row-patch.ts`).
package enum RowPatchOp: Sendable, Hashable, JSONCodable {
  /// Inserts or replaces the row.
  case put(tableName: String, value: RowRecord)
  /// Merges `merge` into the row with primary key `id`. `merge` is a
  /// `jsonObjectSchema`, which checks `typeof v === 'object'` and so admits
  /// an array as well as an object; it is kept as sent. `constrain` limits
  /// the columns the row keeps.
  case update(
    tableName: String, id: PrimaryKeyValueRecord, merge: JSONValue?, constrain: [String]?)
  /// Deletes the row with primary key `id`.
  case del(tableName: String, id: PrimaryKeyValueRecord)
  /// Deletes every row.
  case clear

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "row patch op")
    switch try r.string("op") {
    case "put":
      self = .put(
        tableName: try r.string("tableName"), value: try r.required("value", JSONDecode.row))
    case "update":
      self = .update(
        tableName: try r.string("tableName"),
        id: try r.required("id", JSONDecode.primaryKeyValueRecord),
        merge: try r.optional("merge", JSONDecode.jsonObject),
        constrain: try r.optional("constrain", JSONDecode.strings))
    case "del":
      self = .del(
        tableName: try r.string("tableName"),
        id: try r.required("id", JSONDecode.primaryKeyValueRecord))
    case "clear": self = .clear
    case let op: throw DecodingError("Invalid row patch op \"\(op)\"", path: ["op"])
    }
  }

  package var json: JSONValue {
    switch self {
    case .put(let tableName, let value):
      return ["op": "put", "tableName": .string(tableName), "value": .object(value)]
    case .update(let tableName, let id, let merge, let constrain):
      var o = JSONObjectBuilder()
      o.set("op", "update")
      o.set("tableName", tableName)
      o.set("id", .object(id))
      o.set("merge", merge)
      o.set("constrain", constrain.map(JSONValue.strings))
      return o.value
    case .del(let tableName, let id):
      return ["op": "del", "tableName": .string(tableName), "id": .object(id)]
    case .clear:
      return ["op": "clear"]
    }
  }
}

/// `rowsPatchSchema`.
package typealias RowsPatch = [RowPatchOp]

extension JSONDecode {
  /// `rowSchema` (`packages/zero-protocol/src/data.ts`): a record of JSON
  /// values.
  static func row(_ v: JSONValue) throws(DecodingError) -> RowRecord {
    try object(v)
  }
}
