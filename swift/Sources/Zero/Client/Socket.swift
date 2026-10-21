/// What the engine sees of a WebSocket, on the main actor: it opens one per
/// connection attempt, sends text frames, and is told about frames and the
/// close. This is the shape of the browser `WebSocket` the JS client uses.
@MainActor
public protocol SocketFactory: AnyObject {
  /// Starts opening a WebSocket to `url`, sending `secProtocol` (when
  /// non-nil) as the `Sec-WebSocket-Protocol` header. Events arrive on
  /// `events` until the engine closes the socket.
  func open(url: String, secProtocol: String?, events: any SocketEvents) -> any SocketConnection
}

/// An open or opening WebSocket.
@MainActor
public protocol SocketConnection: AnyObject {
  func send(_ text: String)
  /// Closes the socket. No more events are delivered after this.
  func close(code: Int?)
}

/// Receives the events of one socket.
@MainActor
public protocol SocketEvents: AnyObject {
  func socketDidOpen()
  func socketDidReceive(_ message: SocketMessage)
  func socketDidClose(code: Int, reason: String, wasClean: Bool)
}

/// Runs an async `Transport` as a `SocketFactory`.
@MainActor
public final class TransportSocketFactory: SocketFactory {
  let transport: any Transport

  public init(_ transport: any Transport) { self.transport = transport }

  public func open(
    url: String, secProtocol: String?, events: any SocketEvents
  )
    -> any SocketConnection
  {
    TransportConnection(transport: transport, url: url, secProtocol: secProtocol, events: events)
  }
}

/// One socket of a `Transport`. Frames are written in the order they are
/// sent, and the close after them.
@MainActor
final class TransportConnection: SocketConnection {
  private enum Outgoing: Sendable {
    case text(String)
    case close(Int)
  }

  private weak var events: (any SocketEvents)?
  private var closed = false
  private let outgoing: AsyncStream<Outgoing>.Continuation

  init(transport: any Transport, url: String, secProtocol: String?, events: any SocketEvents) {
    self.events = events
    let (frames, outgoing) = AsyncStream<Outgoing>.makeStream()
    self.outgoing = outgoing
    Task { @MainActor [weak self] in
      let socket: any Socket
      do {
        socket = try await transport.connect(url: url, secProtocol: secProtocol)
      } catch {
        self?.finish(SocketClosed(error))
        return
      }
      guard let self, !self.closed else {
        await socket.close(code: 1000, reason: "")
        return
      }
      // One writer keeps the frames in order; each send in a task of its
      // own could overtake the one before.
      Task {
        for await frame in frames {
          switch frame {
          case .text(let text):
            try? await socket.send(.text(text))
          case .close(let code):
            await socket.close(code: code, reason: "")
            return
          }
        }
      }
      self.events?.socketDidOpen()
      do {
        while let message = try await socket.receive() {
          if self.closed { return }
          self.events?.socketDidReceive(message)
        }
        self.finish(SocketClosed(code: 1000, reason: "", wasClean: true))
      } catch {
        self.finish(SocketClosed(error))
      }
    }
  }

  func send(_ text: String) {
    guard !closed else { return }
    outgoing.yield(.text(text))
  }

  func close(code: Int?) {
    guard !closed else { return }
    closed = true
    events = nil
    outgoing.yield(.close(code ?? 1000))
    outgoing.finish()
  }

  private func finish(_ close: SocketClosed) {
    guard !closed else { return }
    closed = true
    outgoing.finish()
    let events = self.events
    self.events = nil
    events?.socketDidClose(code: close.code, reason: close.reason, wasClean: close.wasClean)
  }
}
