import Zero

/// Opens in-memory sockets and keeps every one, in opening order, so a test
/// can play the server (`MockSocket` in
/// `packages/zero-client/src/client/test-utils.ts`).
@MainActor
public final class MockSocketFactory: SocketFactory {
  public private(set) var sockets: [MockSocket] = []

  public init() {}

  public func open(
    url: String, secProtocol: String?, events: any SocketEvents
  )
    -> any SocketConnection
  {
    let socket = MockSocket(url: url, secProtocol: secProtocol, events: events)
    sockets.append(socket)
    return socket
  }
}

/// A socket the test drives: it records what the client sends and delivers
/// what the test receives on the server's behalf.
@MainActor
public final class MockSocket: SocketConnection {
  public let url: String
  public let secProtocol: String?
  /// The text frames the client sent, oldest first.
  public private(set) var messages: [String] = []
  /// Whether the client closed the socket.
  public private(set) var closed = false
  public private(set) var closeCode: Int?
  private weak var events: (any SocketEvents)?

  init(url: String, secProtocol: String?, events: any SocketEvents) {
    self.url = url
    self.secProtocol = secProtocol
    self.events = events
  }

  public func send(_ text: String) { messages.append(text) }

  public func close(code: Int?) {
    closed = true
    closeCode = code
  }

  /// Delivers a frame from the server.
  public func receive(_ message: SocketMessage) { events?.socketDidReceive(message) }

  public func receive(text: String) { receive(.text(text)) }

  /// The socket closes from the server or network side.
  public func serverClose(code: Int = 1006, reason: String = "", wasClean: Bool = false) {
    events?.socketDidClose(code: code, reason: reason, wasClean: wasClean)
  }

  public func open() { events?.socketDidOpen() }
}
