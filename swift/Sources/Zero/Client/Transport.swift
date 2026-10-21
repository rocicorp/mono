/// A WebSocket message.
public enum SocketMessage: Sendable, Hashable {
  case text(String)
  case binary([UInt8])
}

/// Opens the sync socket. The engine never touches a socket itself: the Apple
/// layer implements this with `URLSessionWebSocketTask`, and tests replay
/// scenarios through an in-memory transport.
public protocol Transport: Sendable {
  /// Opens a WebSocket to `url`, sending `secProtocol` (when non-nil) as
  /// the `Sec-WebSocket-Protocol` header.
  func connect(url: String, secProtocol: String?) async throws -> any Socket
}

/// An open WebSocket.
public protocol Socket: AnyObject, Sendable {
  func send(_ message: SocketMessage) async throws
  /// The next message, or nil once the socket has closed cleanly. Throws
  /// `SocketClosed` to report how it closed otherwise; any other error is
  /// an abrupt close.
  func receive() async throws -> SocketMessage?
  func close(code: Int, reason: String) async
}

/// How a socket closed: the close frame's code and reason, and whether the
/// closing handshake completed (the browser `CloseEvent`).
public struct SocketClosed: Error, Sendable, Hashable {
  public var code: Int
  public var reason: String
  public var wasClean: Bool

  public init(code: Int, reason: String, wasClean: Bool) {
    self.code = code
    self.reason = reason
    self.wasClean = wasClean
  }

  /// `error` as a close: itself, or an abrupt close (1006).
  public init(_ error: any Error) {
    if let closed = error as? SocketClosed {
      self = closed
    } else {
      self.init(code: 1006, reason: String(describing: error), wasClean: false)
    }
  }
}

/// The query parameters of the sync connect URL, in the order JS sets them
/// (`createConnectionURL` in `packages/zero-client/src/client/zero.ts`).
package struct ConnectionURL: Sendable, Hashable {
  package var socketOrigin: String
  package var clientID: String
  package var clientGroupID: String
  package var userID: String?
  /// The cookie of the client's last server snapshot, nil for none.
  package var baseCookie: String?
  /// The `ts` parameter, `performance.now()` in JS.
  package var timestamp: Double
  package var lastMutationID: Double
  package var wsid: String
  package var profileID: String
  package var debugPerf: Bool
  package var additionalParams: [(String, String)]

  package init(
    socketOrigin: String, clientID: String, clientGroupID: String, userID: String? = nil,
    baseCookie: String? = nil, timestamp: Double, lastMutationID: Double, wsid: String,
    profileID: String, debugPerf: Bool = false, additionalParams: [(String, String)] = []
  ) {
    self.socketOrigin = socketOrigin
    self.clientID = clientID
    self.clientGroupID = clientGroupID
    self.userID = userID
    self.baseCookie = baseCookie
    self.timestamp = timestamp
    self.lastMutationID = lastMutationID
    self.wsid = wsid
    self.profileID = profileID
    self.debugPerf = debugPerf
    self.additionalParams = additionalParams
  }

  package static func == (a: ConnectionURL, b: ConnectionURL) -> Bool { a.string == b.string }
  package func hash(into hasher: inout Hasher) { hasher.combine(string) }

  package var string: String {
    var params: [(String, String)] = [
      ("clientID", clientID), ("clientGroupID", clientGroupID),
    ]
    if let userID { params.append(("userID", userID)) }
    params += [
      ("baseCookie", baseCookie ?? ""), ("ts", JSNumber.format(timestamp)),
      ("lmid", JSNumber.format(lastMutationID)), ("wsid", wsid), ("profileID", profileID),
    ]
    if debugPerf { params.append(("debugPerf", "true")) }
    // A parameter the client sets wins; JS warns and skips the duplicate.
    for (k, v) in additionalParams where !params.contains(where: { $0.0 == k }) {
      params.append((k, v))
    }
    let path = "/sync/v\(protocolVersion)/connect"
    let base = socketOrigin.hasSuffix("/") ? socketOrigin + path.dropFirst() : socketOrigin + path
    return base + "?"
      + params.map { formEncode($0.0) + "=" + formEncode($0.1) }.joined(separator: "&")
  }
}

/// `application/x-www-form-urlencoded`, as `URLSearchParams` writes it.
func formEncode(_ s: String) -> String {
  var out = ""
  for byte in s.utf8 {
    switch byte {
    case UInt8(ascii: "a")...UInt8(ascii: "z"), UInt8(ascii: "A")...UInt8(ascii: "Z"),
      UInt8(ascii: "0")...UInt8(ascii: "9"), UInt8(ascii: "*"), UInt8(ascii: "-"),
      UInt8(ascii: "."), UInt8(ascii: "_"):
      out.unicodeScalars.append(Unicode.Scalar(byte))
    case UInt8(ascii: " "):
      out += "+"
    default:
      let hex = Array("0123456789ABCDEF".unicodeScalars)
      out += "%"
      out.unicodeScalars.append(hex[Int(byte >> 4)])
      out.unicodeScalars.append(hex[Int(byte & 0xF)])
    }
  }
  return out
}
