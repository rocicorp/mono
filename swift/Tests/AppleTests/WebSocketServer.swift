#if canImport(Network)
  import Foundation
  import Network
  import Zero

  /// A value behind a lock.
  final class Locked<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Value

    init(_ value: Value) { self.value = value }

    func withLock<R>(_ body: (inout Value) -> R) -> R {
      lock.lock()
      defer { lock.unlock() }
      return body(&value)
    }
  }

  /// A WebSocket server on the loopback interface, for the URLSession
  /// transport to talk to.
  final class WebSocketServer: Sendable {
    enum Event: Sendable {
      case connected(Peer, protocols: [String])
      case message(Peer, SocketMessage)
      case closed(Peer, code: Int)
    }

    /// One client connection.
    final class Peer: Sendable {
      let connection: NWConnection

      init(_ connection: NWConnection) { self.connection = connection }

      func send(_ message: SocketMessage) {
        let metadata: NWProtocolWebSocket.Metadata
        let data: Data
        switch message {
        case .text(let text):
          metadata = NWProtocolWebSocket.Metadata(opcode: .text)
          data = Data(text.utf8)
        case .binary(let bytes):
          metadata = NWProtocolWebSocket.Metadata(opcode: .binary)
          data = Data(bytes)
        }
        let context = NWConnection.ContentContext(identifier: "frame", metadata: [metadata])
        connection.send(
          content: data, contentContext: context, isComplete: true, completion: .idempotent)
      }

      /// Sends a close frame with an application code (4000-4999).
      func close(code: UInt16, reason: String) {
        let metadata = NWProtocolWebSocket.Metadata(opcode: .close)
        metadata.closeCode = .privateCode(code)
        let context = NWConnection.ContentContext(identifier: "close", metadata: [metadata])
        connection.send(
          content: Data(reason.utf8), contentContext: context, isComplete: true,
          completion: .idempotent)
      }
    }

    private let listener: NWListener
    private let queue = DispatchQueue(label: "test.websocket")
    let events: AsyncStream<Event>
    private let continuation: AsyncStream<Event>.Continuation
    private let protocols = Locked<[String]>([])

    init() throws {
      (events, continuation) = AsyncStream<Event>.makeStream()
      let parameters = NWParameters.tcp
      let options = NWProtocolWebSocket.Options()
      options.autoReplyPing = true
      options.maximumMessageSize = 64 << 20
      let protocols = self.protocols
      options.setClientRequestHandler(queue) { subprotocols, _ in
        protocols.withLock { $0 = subprotocols }
        return NWProtocolWebSocket.Response(
          status: .accept, subprotocol: subprotocols.first, additionalHeaders: nil)
      }
      parameters.defaultProtocolStack.applicationProtocols.insert(options, at: 0)
      listener = try NWListener(using: parameters, on: .any)
    }

    /// Starts listening; returns the port.
    func start() async throws -> UInt16 {
      let continuation = self.continuation
      let protocols = self.protocols
      let queue = self.queue
      listener.newConnectionHandler = { connection in
        let peer = Peer(connection)
        connection.stateUpdateHandler = { state in
          if case .ready = state {
            continuation.yield(.connected(peer, protocols: protocols.withLock { $0 }))
          }
        }
        connection.start(queue: queue)
        Self.receive(peer, continuation)
      }
      return try await withCheckedThrowingContinuation {
        (c: CheckedContinuation<UInt16, any Error>) in
        let resumed = Locked(false)
        let first: @Sendable () -> Bool = {
          resumed.withLock { r in
            defer { r = true }
            return !r
          }
        }
        listener.stateUpdateHandler = { [listener] state in
          switch state {
          case .ready: if first() { c.resume(returning: listener.port?.rawValue ?? 0) }
          case .failed(let error): if first() { c.resume(throwing: error) }
          default: break
          }
        }
        listener.start(queue: queue)
      }
    }

    func stop() {
      listener.cancel()
      continuation.finish()
    }

    private static func receive(_ peer: Peer, _ events: AsyncStream<Event>.Continuation) {
      peer.connection.receiveMessage { data, context, _, error in
        if error != nil {
          events.yield(.closed(peer, code: 1006))
          return
        }
        let metadata =
          context?.protocolMetadata(definition: NWProtocolWebSocket.definition)
          as? NWProtocolWebSocket.Metadata
        switch metadata?.opcode {
        case .text?:
          events.yield(.message(peer, .text(String(decoding: data ?? Data(), as: UTF8.self))))
        case .binary?:
          events.yield(.message(peer, .binary([UInt8](data ?? Data()))))
        case .close?:
          var code = 1005
          switch metadata?.closeCode {
          case .protocolCode(let defined)?: code = Int(defined.rawValue)
          case .applicationCode(let c)?, .privateCode(let c)?: code = Int(c)
          default: break
          }
          events.yield(.closed(peer, code: code))
          return
        default:
          break
        }
        receive(peer, events)
      }
    }
  }
#endif
