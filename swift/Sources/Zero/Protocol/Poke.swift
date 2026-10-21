/// The range of schema versions the server supports.
package struct SchemaVersions: Sendable, Hashable {
  package var minSupportedVersion: Double
  package var maxSupportedVersion: Double

  package init(minSupportedVersion: Double, maxSupportedVersion: Double) {
    self.minSupportedVersion = minSupportedVersion
    self.maxSupportedVersion = maxSupportedVersion
  }
}

/// Starts a poke (`pokeStartBodySchema` in
/// `packages/zero-protocol/src/poke.ts`).
package struct PokeStartBody: Sendable, Hashable, JSONCodable {
  package var pokeID: String
  /// The cookie the poke applies to; `nil` for a client that has never
  /// pulled, as Replicache starts clients with a `null` cookie.
  package var baseCookie: Version?
  /// Always set when the poke has a `rowsPatch`.
  package var schemaVersions: SchemaVersions?
  package var timestamp: Double?

  package init(
    pokeID: String, baseCookie: Version?, schemaVersions: SchemaVersions? = nil,
    timestamp: Double? = nil
  ) {
    self.pokeID = pokeID
    self.baseCookie = baseCookie
    self.schemaVersions = schemaVersions
    self.timestamp = timestamp
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "pokeStart body")
    pokeID = try r.string("pokeID")
    baseCookie = try r.required("baseCookie", NullableVersion.decode)
    schemaVersions = try r.optional("schemaVersions") { (v) throws(DecodingError) in
      let s = try JSONObjectReader(v, "schema versions")
      return SchemaVersions(
        minSupportedVersion: try s.number("minSupportedVersion"),
        maxSupportedVersion: try s.number("maxSupportedVersion"))
    }
    timestamp = try r.optionalNumber("timestamp")
  }

  package var json: JSONValue {
    var o = JSONObjectBuilder()
    o.set("pokeID", pokeID)
    o.set("baseCookie", NullableVersion.json(baseCookie))
    o.set(
      "schemaVersions",
      schemaVersions.map {
        [
          "minSupportedVersion": .number($0.minSupportedVersion),
          "maxSupportedVersion": .number($0.maxSupportedVersion),
        ]
      })
    o.set("timestamp", timestamp)
    return o.value
  }
}

/// One part of a poke (`pokePartBodySchema` in
/// `packages/zero-protocol/src/poke.ts`).
package struct PokePartBody: Sendable, Hashable, JSONCodable {
  package var pokeID: String
  /// The new last mutation ID of each client that changed.
  package var lastMutationIDChanges: [String: Double]?
  /// Changes to each client's desired queries, by client ID.
  package var desiredQueriesPatches: [String: QueriesPatch]?
  /// Changes to the queries whose rows `rowsPatch` syncs.
  package var gotQueriesPatch: QueriesPatch?
  package var rowsPatch: RowsPatch?
  package var mutationsPatch: MutationsPatch?

  package init(
    pokeID: String, lastMutationIDChanges: [String: Double]? = nil,
    desiredQueriesPatches: [String: QueriesPatch]? = nil, gotQueriesPatch: QueriesPatch? = nil,
    rowsPatch: RowsPatch? = nil, mutationsPatch: MutationsPatch? = nil
  ) {
    self.pokeID = pokeID
    self.lastMutationIDChanges = lastMutationIDChanges
    self.desiredQueriesPatches = desiredQueriesPatches
    self.gotQueriesPatch = gotQueriesPatch
    self.rowsPatch = rowsPatch
    self.mutationsPatch = mutationsPatch
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "pokePart body")
    lastMutationIDChanges = try r.optional("lastMutationIDChanges", JSONDecode.numberRecord)
    desiredQueriesPatches = try r.optional("desiredQueriesPatches") { (v) throws(DecodingError) in
      try JSONDecode.record(v, JSONDecode.queriesPatch)
    }
    gotQueriesPatch = try r.optional("gotQueriesPatch", JSONDecode.queriesPatch)
    rowsPatch = try r.optional("rowsPatch") { (v) throws(DecodingError) in
      try JSONDecode.array(v, RowPatchOp.init(json:))
    }
    mutationsPatch = try r.optional("mutationsPatch") { (v) throws(DecodingError) in
      try JSONDecode.array(v, MutationPatchOp.init(json:))
    }
    pokeID = try r.string("pokeID")
  }

  package var json: JSONValue {
    var o = JSONObjectBuilder()
    o.set("lastMutationIDChanges", lastMutationIDChanges.map(JSONValue.numberRecord))
    o.set(
      "desiredQueriesPatches",
      desiredQueriesPatches.map { JSONValue.object($0.mapValues { .array($0.map(\.json)) }) })
    o.set("gotQueriesPatch", gotQueriesPatch.map { JSONValue.array($0.map(\.json)) })
    o.set("rowsPatch", rowsPatch.map { JSONValue.array($0.map(\.json)) })
    o.set("mutationsPatch", mutationsPatch.map { JSONValue.array($0.map(\.json)) })
    o.set("pokeID", pokeID)
    return o.value
  }
}

/// Ends a poke (`pokeEndBodySchema` in `packages/zero-protocol/src/poke.ts`).
package struct PokeEndBody: Sendable, Hashable, JSONCodable {
  package var pokeID: String
  /// The cookie after the poke. Ignore it, and it may be empty, if
  /// `cancel` is set.
  package var cookie: Version
  /// Discard the poke without applying it.
  package var cancel: Bool?

  package init(pokeID: String, cookie: Version, cancel: Bool? = nil) {
    self.pokeID = pokeID
    self.cookie = cookie
    self.cancel = cancel
  }

  package init(json: JSONValue) throws(DecodingError) {
    let r = try JSONObjectReader(json, "pokeEnd body")
    pokeID = try r.string("pokeID")
    cookie = try r.string("cookie")
    cancel = try r.optionalBool("cancel")
  }

  package var json: JSONValue {
    var o = JSONObjectBuilder()
    o.set("pokeID", pokeID)
    o.set("cookie", cookie)
    o.set("cancel", cancel)
    return o.value
  }
}

/// The binary poke chunks of protocol 52 and later
/// (`packages/zero-protocol/src/poke.ts`). Between `pokeStart` and `pokeEnd`
/// zero-cache may send the poke's parts as binary messages instead of
/// `pokePart` messages: each is a ``messageType`` byte and then the next
/// bytes of the UTF-8 JSON of a `PokePartBody[]`. A chunk can end inside a
/// UTF-8 sequence.
package enum PokeChunk {
  /// `POKE_CHUNK_MESSAGE_TYPE`.
  package static let messageType: UInt8 = 0x00
  /// `POKE_CHUNK_PROTOCOL_VERSION`.
  package static let protocolVersion = 52

  /// The payload of a binary message. Throws for a message of another type,
  /// which the client treats as an invalid message
  /// (`#onMessage` in `packages/zero-client/src/client/zero.ts`).
  package static func payload(
    ofBinaryMessage message: [UInt8]
  ) throws(DecodingError)
    -> ArraySlice<UInt8>
  {
    guard let type = message.first else {
      throw DecodingError("Unknown binary message type: undefined")
    }
    guard type == messageType else {
      throw DecodingError("Unknown binary message type: \(type)")
    }
    return message.dropFirst()
  }

  /// Decodes the payloads of a poke's chunks, in order, into its parts
  /// (`decodePokeChunks` in `packages/zero-client/src/client/zero-poke-handler.ts`):
  /// strict UTF-8, a leading BOM dropped as `TextDecoder` does, then
  /// `pokePartsSchema`.
  package static func decode(
    _ chunks: some Sequence<some Collection<UInt8>>
  ) throws(DecodingError)
    -> [PokePartBody]
  {
    var bytes: [UInt8] = []
    for chunk in chunks { bytes.append(contentsOf: chunk) }
    let payload = bytes.starts(with: utf8BOM) ? bytes[3...] : bytes[...]
    guard let text = String(validating: payload, as: UTF8.self) else {
      throw DecodingError("The encoded data was not valid for encoding utf-8")
    }
    return try JSONDecode.array(try JSONValue.parseMessage(text), PokePartBody.init(json:))
  }
}
