/// A merged poke, ready to apply (`PokeInternal`).
struct MergedPoke {
  var baseCookie: JSONValue
  var cookie: JSONValue
  var lastMutationIDChanges: [String: Double]
  var patch: [KVPatchOp]
}

/// Assembles the multi-part pokes of the sync protocol and applies them at
/// most once per scheduler turn, merging the pokes that arrived in between
/// (`packages/zero-client/src/client/zero-poke-handler.ts`).
@MainActor
final class PokeHandler {
  struct PokeEndResult {
    var lastMutationIDChangeForSelf: Double?
    var hasRows: Bool
  }

  private struct Receiving {
    var pokeStart: PokeStartBody
    var parts: [PokePartBody] = []
    var chunks: [ArraySlice<UInt8>] = []
  }

  private struct Received {
    var pokeStart: PokeStartBody
    var parts: [PokePartBody]
    var pokeEnd: PokeEndBody
  }

  private let clientID: String
  private let schema: ZeroSchema
  private let serverToClient: NameMapper
  private let scheduler: any Scheduler
  /// Applies a merged poke; throws when it does not apply.
  private let apply: (MergedPoke) throws -> Void
  private let onPokeError: (String) -> Void
  private let onApplied: (MergedPoke) throws -> Void

  private var receiving: Receiving?
  private var buffer: [Received] = []
  private var playbackRunning = false

  init(
    clientID: String, schema: ZeroSchema, scheduler: any Scheduler,
    apply: @escaping (MergedPoke) throws -> Void,
    onApplied: @escaping (MergedPoke) throws -> Void,
    onPokeError: @escaping (String) -> Void
  ) {
    self.clientID = clientID
    self.schema = schema
    self.serverToClient = NameMapper.serverToClient(schema)
    self.scheduler = scheduler
    self.apply = apply
    self.onApplied = onApplied
    self.onPokeError = onPokeError
  }

  func handlePokeStart(_ pokeStart: PokeStartBody) {
    if let receiving {
      handlePokeError(
        "pokeStart \(Self.text(pokeStart)) while still receiving  \(Self.text(receiving.pokeStart)) "
      )
      return
    }
    receiving = Receiving(pokeStart: pokeStart)
  }

  /// Returns the poke's last mutation ID change for this client.
  func handlePokePart(_ part: PokePartBody) -> Double? {
    guard part.pokeID == receiving?.pokeStart.pokeID else {
      handlePokeError(
        "pokePart for \(part.pokeID), when receiving \(receiving?.pokeStart.pokeID ?? "undefined")")
      return nil
    }
    if !receiving!.chunks.isEmpty {
      handlePokeError("received both pokePart and binary poke chunks")
      return nil
    }
    receiving!.parts.append(part)
    return part.lastMutationIDChanges?[clientID]
  }

  func handlePokeChunk(_ chunk: ArraySlice<UInt8>) {
    guard receiving != nil else {
      handlePokeError("received a binary poke chunk without pokeStart")
      return
    }
    if !receiving!.parts.isEmpty {
      handlePokeError("received both pokePart and binary poke chunks")
      return
    }
    receiving!.chunks.append(chunk)
  }

  func handlePokeEnd(_ pokeEnd: PokeEndBody) -> PokeEndResult? {
    guard let receiving, pokeEnd.pokeID == receiving.pokeStart.pokeID else {
      handlePokeError(
        "pokeEnd for \(pokeEnd.pokeID), when receiving \(receiving?.pokeStart.pokeID ?? "undefined")"
      )
      return nil
    }
    if pokeEnd.cancel == true {
      self.receiving = nil
      return nil
    }
    var parts = receiving.parts
    if !receiving.chunks.isEmpty {
      do {
        parts = try PokeChunk.decode(receiving.chunks)
      } catch {
        // `String(e)` of what `TextDecoder` and valita throw.
        handlePokeError("TypeError: " + valitaMessage(error))
        return nil
      }
    }
    var result = PokeEndResult(lastMutationIDChangeForSelf: nil, hasRows: false)
    for part in parts {
      if let lmid = part.lastMutationIDChanges?[clientID] {
        result.lastMutationIDChangeForSelf = lmid
      }
      if let rows = part.rowsPatch, !rows.isEmpty { result.hasRows = true }
    }
    buffer.append(Received(pokeStart: receiving.pokeStart, parts: parts, pokeEnd: pokeEnd))
    self.receiving = nil
    if !playbackRunning { startPlayback() }
    return result
  }

  func handleDisconnect() { clear() }

  private func startPlayback() {
    playbackRunning = true
    _ = scheduler.schedule(after: 0) { [weak self] in self?.scheduledCallback() }
  }

  private func scheduledCallback() {
    if buffer.isEmpty {
      playbackRunning = false
      return
    }
    _ = scheduler.schedule(after: 0) { [weak self] in self?.scheduledCallback() }
    processPokesForFrame()
  }

  private func processPokesForFrame() {
    do {
      let merged = try merge(buffer)
      buffer.removeAll()
      guard let merged else { return }
      try apply(merged)
      try onApplied(merged)
    } catch {
      handlePokeError(Self.errorString(error))
    }
  }

  /// `String(error)` of the JS error the failure stands for.
  static func errorString(_ error: any Error) -> String {
    if let e = error as? PokeError { return "Error: " + e.message }
    return "Error: " + String(describing: error)
  }

  private func handlePokeError(_ message: String) {
    clear()
    onPokeError(message)
  }

  private func clear() {
    receiving = nil
    buffer.removeAll()
  }

  /// `mergePokes`.
  private func merge(_ pokes: [Received]) throws -> MergedPoke? {
    guard let first = pokes.first, let last = pokes.last else { return nil }
    var merged = MergedPoke(
      baseCookie: first.pokeStart.baseCookie.map { .string($0) } ?? .null,
      cookie: .string(last.pokeEnd.cookie), lastMutationIDChanges: [:], patch: [])
    var prevPokeEnd: PokeEndBody?
    for poke in pokes {
      if let prev = prevPokeEnd, let base = poke.pokeStart.baseCookie,
        StringCompare.utf16(base, prev.cookie) > 0
      {
        throw PokeError(
          "unexpected cookie gap \(Self.text(prev)) \(Self.text(poke.pokeStart))"
        )
      }
      prevPokeEnd = poke.pokeEnd
      for part in poke.parts {
        if let changes = part.lastMutationIDChanges {
          for (clientID, lmid) in changes { merged.lastMutationIDChanges[clientID] = lmid }
        }
        if let patches = part.desiredQueriesPatches {
          for (clientID, patch) in patches.sorted(by: { StringCompare.utf16Less($0.key, $1.key) }) {
            for op in patch {
              merged.patch.append(
                Self.queryPatchOp(op) { Keys.desiredQueriesKey(clientID: clientID, hash: $0) })
            }
          }
        }
        if let patch = part.gotQueriesPatch {
          for op in patch { merged.patch.append(Self.queryPatchOp(op, Keys.gotQueriesKey)) }
        }
        if let rows = part.rowsPatch {
          for op in rows {
            if let p = try rowPatchOp(op) { merged.patch.append(p) }
          }
        }
        if let mutations = part.mutationsPatch {
          for op in mutations {
            switch op {
            case .put(let response):
              merged.patch.append(
                .put(
                  key: Keys.mutationResponseKey(
                    clientID: response.id.clientID, id: response.id.id),
                  value: response.result.json))
            case .del(let id):
              merged.patch.append(
                .del(key: Keys.mutationResponseKey(clientID: id.clientID, id: id.id)))
            }
          }
        }
      }
    }
    return merged
  }

  /// `JSON.stringify` of a body, its keys in the order zero-cache sends them.
  static func text(_ b: PokeStartBody) -> String {
    let json = b.json
    return orderedJSON(json, ["pokeID", "baseCookie", "schemaVersions", "timestamp"])
  }

  static func text(_ b: PokeEndBody) -> String {
    orderedJSON(b.json, ["pokeID", "cookie", "cancel"])
  }

  static func orderedJSON(_ json: JSONValue, _ order: [String]) -> String {
    guard case .object(let o) = json else { return JSONSerializer.stringify(json) }
    var parts: [String] = []
    for key in order {
      guard let v = o[key] else { continue }
      var s = ""
      JSONSerializer.writeString(key, into: &s)
      parts.append(s + ":" + JSONSerializer.stringify(v))
    }
    return "{" + parts.joined(separator: ",") + "}"
  }

  private static func queryPatchOp(_ op: QueriesPatchOp, _ key: (String) -> String) -> KVPatchOp {
    switch op {
    case .clear: .clear
    case .del(let hash): .del(key: key(hash))
    case .put(let hash, _): .put(key: key(hash), value: .null)
    }
  }

  private func rowPatchOp(_ op: RowPatchOp) throws -> KVPatchOp? {
    switch op {
    case .clear:
      return .clear
    case .put(let serverTable, let value):
      guard let table = serverToClient.tableNameIfKnown(serverTable) else { return nil }
      let row = try serverToClient.row(serverTable, value)
      return .put(key: try key(table, row), value: .object(row))
    case .del(let serverTable, let id):
      guard let table = serverToClient.tableNameIfKnown(serverTable) else { return nil }
      return .del(key: try key(table, try serverToClient.row(serverTable, id)))
    case .update(let serverTable, let id, let merge, let constrain):
      guard let table = serverToClient.tableNameIfKnown(serverTable) else { return nil }
      let mergeRow = try merge.map { m -> RowRecord in
        try serverToClient.row(serverTable, m.objectValue ?? [:])
      }
      return .update(
        key: try key(table, try serverToClient.row(serverTable, id)), merge: mergeRow,
        constrain: try constrain.map { try serverToClient.columns(serverTable, $0) })
    }
  }

  private func key(_ table: String, _ row: RowRecord) throws -> String {
    try Keys.primaryKeyString(table: table, primaryKey: schema.tables[table]!.primaryKey, row: row)
  }
}

/// A poke that cannot be applied. `message` is the JS error's message.
struct PokeError: Error, CustomStringConvertible {
  let message: String
  init(_ message: String) { self.message = message }
  var description: String { message }
}
