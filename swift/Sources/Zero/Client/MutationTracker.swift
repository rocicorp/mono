/// An error a mutator raised for the app to handle, or that the API server
/// reported for a mutation (`ApplicationError` in
/// `packages/zero-protocol/src/application-error.ts`). Throw it from a
/// mutator to give the caller a message and details.
public struct ApplicationError: Error, Sendable, Hashable, CustomStringConvertible {
  public var message: String
  public var details: JSONValue?

  public init(_ message: String, details: JSONValue? = nil) {
    self.message = message
    self.details = details
  }

  public var description: String { message }

  /// `wrapWithApplicationError`.
  static func wrapping(_ error: any Error) -> ApplicationError {
    if let e = error as? ApplicationError { return e }
    return ApplicationError(errorMessage(error))
  }
}

/// `getErrorMessage` for a Swift error.
func errorMessage(_ error: any Error) -> String {
  if let e = error as? ApplicationError { return e.message }
  if let e = error as? ConnectionError { return e.message }
  return String(describing: error)
}

/// Tracks the mutations waiting for the server's verdict and settles their
/// `server` results (`packages/zero-client/src/client/mutation-tracker.ts`).
@MainActor
final class MutationTracker {
  final class Entry {
    var mutationID: Double?
    let server: Deferred<JSONValue?>
    init(_ server: Deferred<JSONValue?>) { self.server = server }
  }

  /// By ephemeral ID (a `Map` in JS).
  private var outstanding: [Int: Entry] = [:]
  /// The ephemeral IDs in insertion order, which is mutation ID order, from
  /// `head` on. Settled IDs stay until the head passes them.
  private var order: [Int] = []
  private var head = 0
  private var ephemeralIDsByMutationID: [Double: Int] = [:]
  private var allMutationsAppliedListeners: [() -> Void] = []
  private var nextEphemeralID = 0
  private var clientID: String?
  private var largestOutstandingMutationID: Double = 0
  private var currentMutationID: Double = 0
  /// The mutations sent to the server whose outcome hasn't been reported,
  /// by mutation ID. Unlike `outstanding`, they stay when the connection
  /// gives up on them: they're sent again on reconnect, and their outcome
  /// is reported then.
  private var unreported: [Double: (name: String, args: JSONValue?)] = [:]
  /// Hears each mutation's outcome from the server once.
  var onOutcome: ((MutationServerOutcome) -> Void)?

  private let ackMutations: (MutationID) -> Void
  private let onFatalError: (ConnectionError) -> Void

  init(
    ackMutations: @escaping (MutationID) -> Void,
    onFatalError: @escaping (ConnectionError) -> Void
  ) {
    self.ackMutations = ackMutations
    self.onFatalError = onFatalError
  }

  func setClientID(_ clientID: String) { self.clientID = clientID }

  var size: Int { outstanding.count }

  func trackMutation() -> (ephemeralID: Int, server: Deferred<JSONValue?>) {
    nextEphemeralID += 1
    let server = Deferred<JSONValue?>()
    if outstanding.isEmpty {
      order.removeAll(keepingCapacity: true)
      head = 0
    }
    outstanding[nextEphemeralID] = Entry(server)
    order.append(nextEphemeralID)
    return (nextEphemeralID, server)
  }

  func mutationIDAssigned(
    _ ephemeralID: Int, _ mutationID: Double, name: String, args: JSONValue?
  ) {
    guard let entry = entry(ephemeralID) else { return }
    unreported[mutationID] = (name, args)
    entry.mutationID = mutationID
    ephemeralIDsByMutationID[mutationID] = ephemeralID
    largestOutstandingMutationID = max(largestOutstandingMutationID, mutationID)
  }

  /// Rejects a mutation the client could not run. It was not stored.
  func rejectMutation(_ ephemeralID: Int, _ error: any Error) {
    guard let entry = entry(ephemeralID) else { return }
    if let mid = entry.mutationID { unreported[mid] = nil }
    settle(ephemeralID, entry, .failure(ApplicationError.wrapping(error)))
  }

  /// Rejects every outstanding mutation, when the client is disconnected,
  /// in error, or closed.
  func rejectAllOutstandingMutations(_ error: ConnectionError) {
    guard !outstanding.isEmpty else { return }
    for id in order[head...] {
      if let entry = outstanding[id] { settle(id, entry, .failure(error)) }
    }
    order = []
    head = 0
    largestOutstandingMutationID = currentMutationID
    notifyAllMutationsApplied()
  }

  /// The watch of `m/<clientID>/`: mutation results zero-cache poked down.
  func processMutationResponses(_ diff: KVDiff) throws {
    guard let clientID else { return }
    let prefix = Keys.mutationsPrefix + clientID + "/"
    var largest: Double = 0
    for op in diff {
      let suffix = String(op.key.utf8.dropFirst(prefix.utf8.count))!
      guard let mutationID = Double(suffix) else {
        throw ConnectionError.client(
          .internal,
          message: "MutationTracker received a diff with an invalid mutation ID: \(op.key)")
      }
      largest = max(largest, mutationID)
      switch op {
      case .add(_, let value):
        let result = try MutationResult(json: value)
        switch result {
        case .ok(let data): try processMutationOk(clientID, mutationID, data)
        case .error(let e): try processMutationError(clientID, mutationID, e)
        }
      case .del:
        break
      case .change:
        throw ConnectionError.client(
          .internal, message: "MutationTracker does not expect change operations")
      }
    }
    if largest > 0 { ackMutations(MutationID(id: largest, clientID: clientID)) }
  }

  func processPushResponse(_ response: PushResponseBody) throws {
    switch response {
    case .error(let e):
      onFatalError(Self.fatalError(e))
    case .ok(let mutations):
      for m in mutations {
        switch m.result {
        case .ok(let data): try processMutationOk(m.id.clientID, m.id.id, data)
        case .error(let e): try processMutationError(m.id.clientID, m.id.id, e)
        }
      }
    }
  }

  static func fatalError(_ e: PushError) -> ConnectionError {
    let (message, cause): (String, FailureCause) =
      switch e {
      case .unsupportedPushVersion: ("Unsupported push version", .zeroCache(reason: .internal))
      case .unsupportedSchemaVersion: ("Unsupported schema version", .zeroCache(reason: .internal))
      case .http(let status, let details, _):
        (
          "Fetch from API server returned non-OK status \(JSNumber.format(status)): \(details)",
          .http(status: status, bodyPreview: nil)
        )
      case .zeroPusher(let details, _):
        ("ZeroPusher error: \(details)", .zeroCache(reason: .internal))
      }
    return .server(.pushFailed(PushFailedBody(message: message, mutationIDs: [], cause: cause)))
  }

  /// Resolves the mutations the server has applied (`lmidAdvanced`).
  func onConnected(_ lastMutationID: Double) { try? lmidAdvanced(lastMutationID) }

  func lmidAdvanced(_ lastMutationID: Double) throws {
    guard lastMutationID >= currentMutationID else {
      throw PokeError("lmid must be greater than or equal to current lmid")
    }
    if lastMutationID == currentMutationID { return }
    currentMutationID = lastMutationID
    // Applied without a result of their own: they succeeded.
    for mid in unreported.keys.filter({ $0 <= lastMutationID }).sorted() {
      report(mid, .success(nil))
    }
    while head < order.count {
      let id = order[head]
      if let entry = outstanding[id] {
        guard let mid = entry.mutationID, mid <= lastMutationID else { break }
        settle(id, entry, .success(nil))
      }
      head += 1
    }
    if head > 0, head * 2 >= order.count {
      order.removeFirst(head)
      head = 0
    }
    if lastMutationID >= largestOutstandingMutationID { notifyAllMutationsApplied() }
  }

  func onAllMutationsApplied(_ listener: @escaping () -> Void) {
    allMutationsAppliedListeners.append(listener)
  }

  private func notifyAllMutationsApplied() {
    for l in allMutationsAppliedListeners { l() }
  }

  private func entry(_ ephemeralID: Int) -> Entry? { outstanding[ephemeralID] }

  private func processMutationError(
    _ clientID: String, _ mid: Double, _ error: MutationError
  )
    throws
  {
    guard clientID == self.clientID else {
      throw ConnectionError.client(.internal, message: "received mutation for the wrong client")
    }
    let kind = error.error.stringValue ?? ""
    if kind == "alreadyProcessed" { return }
    let failure: any Error
    if kind == "app" {
      failure = ApplicationError(
        error.message?.stringValue ?? "Unknown application error: app", details: error.details)
    } else {
      let message =
        kind == "oooMutation"
        ? "Server reported an out-of-order mutation"
        : "Unknown fallback error with mutation ID \(JSNumber.format(mid)): \(kind)"
      failure = ConnectionError.server(
        .basic(BasicErrorBody(kind: .invalidPush, message: message, origin: .server)))
    }
    if let ephemeralID = ephemeralIDsByMutationID[mid], let entry = entry(ephemeralID) {
      settle(ephemeralID, entry, .failure(failure))
    }
    report(mid, .failure(failure))
    if kind == "oooMutation" {
      onFatalError(
        .server(
          .basic(
            BasicErrorBody(
              kind: .invalidPush, message: "Server reported an out-of-order mutation",
              origin: .server))))
    }
  }

  private func processMutationOk(_ clientID: String, _ mid: Double, _ data: JSONValue?) throws {
    guard clientID == self.clientID else {
      throw ConnectionError.client(.internal, message: "received mutation for the wrong client")
    }
    if let ephemeralID = ephemeralIDsByMutationID[mid], let entry = entry(ephemeralID) {
      settle(ephemeralID, entry, .success(data))
    }
    report(mid, .success(data))
  }

  /// Reports the outcome of mutation `mid`, the first time the server gives
  /// one.
  private func report(_ mid: Double, _ result: Result<JSONValue?, any Error>) {
    guard let m = unreported.removeValue(forKey: mid) else { return }
    let outcome: MutationOutcome =
      switch result {
      case .success(let data): .success(data: data)
      case .failure(let error): Engine.outcome(error)
      }
    onOutcome?(
      MutationServerOutcome(mutationID: Int(mid), name: m.name, args: m.args, outcome: outcome))
  }

  private func settle(_ ephemeralID: Int, _ entry: Entry, _ result: Result<JSONValue?, any Error>) {
    switch result {
    case .success(let data): entry.server.resolve(data)
    case .failure(let error): entry.server.reject(error)
    }
    outstanding[ephemeralID] = nil
    if let mid = entry.mutationID { ephemeralIDsByMutationID[mid] = nil }
  }
}
