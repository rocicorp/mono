import NIOCore
import NIOHTTP1
import NIOPosix
import NIOWebSocket
import Zero

/// A `Transport` over SwiftNIO for end-to-end tests on Linux, where
/// Foundation has no dependable WebSocket: plain `ws://` only, and no TLS.
public struct NIOTransport: Transport {
  public struct Failure: Error, CustomStringConvertible {
    public var description: String
  }

  public init() {}

  private enum Upgrade {
    case websocket(NIOAsyncChannel<WebSocketFrame, WebSocketFrame>)
    case declined
  }

  public func connect(url: String, secProtocol: String?) async throws -> any Socket {
    let (host, port, uri) = try Self.parse(url)
    let upgrade: EventLoopFuture<Upgrade> = try await ClientBootstrap(
      group: MultiThreadedEventLoopGroup.singleton
    )
    .connect(host: host, port: port) { channel in
      channel.eventLoop.makeCompletedFuture {
        let upgrader = NIOTypedWebSocketClientUpgrader<Upgrade>(maxFrameSize: 1 << 24) {
          channel, _ in
          channel.eventLoop.makeCompletedFuture {
            try channel.pipeline.syncOperations.addHandler(
              NIOWebSocketFrameAggregator(
                minNonFinalFragmentSize: 0, maxAccumulatedFrameCount: 1 << 16,
                maxAccumulatedFrameSize: 1 << 26))
            return .websocket(try NIOAsyncChannel(wrappingChannelSynchronously: channel))
          }
        }
        var headers = HTTPHeaders()
        headers.add(name: "Host", value: "\(host):\(port)")
        if let secProtocol { headers.add(name: "Sec-WebSocket-Protocol", value: secProtocol) }
        let head = HTTPRequestHead(version: .http1_1, method: .GET, uri: uri, headers: headers)
        return try channel.pipeline.syncOperations.configureUpgradableHTTPClientPipeline(
          configuration: .init(
            upgradeConfiguration: .init(
              upgradeRequestHead: head, upgraders: [upgrader],
              notUpgradingCompletionHandler: { $0.eventLoop.makeCompletedFuture { .declined } })))
      }
    }
    switch try await upgrade.get() {
    case .websocket(let channel): return NIOSocket(channel)
    case .declined: throw Failure(description: "\(url) declined the WebSocket upgrade")
    }
  }

  /// `ws://host[:port]/path?query` → host, port, and request URI.
  static func parse(_ url: String) throws -> (String, Int, String) {
    guard url.hasPrefix("ws://") else { throw Failure(description: "not a ws:// URL: \(url)") }
    let rest = url.dropFirst("ws://".count)
    let slash = rest.firstIndex(of: "/") ?? rest.endIndex
    let authority = rest[..<slash]
    let uri = slash == rest.endIndex ? "/" : String(rest[slash...])
    if let colon = authority.lastIndex(of: ":"),
      let port = Int(authority[authority.index(after: colon)...])
    {
      return (String(authority[..<colon]), port, uri)
    }
    return (String(authority), 80, uri)
  }
}

/// One reader at a time: `receive` is called by a single task.
final class NIOSocket: Socket, @unchecked Sendable {
  private var inbound: AsyncThrowingStream<SocketMessage, any Error>.AsyncIterator
  private let outbound: AsyncStream<WebSocketFrame>.Continuation
  private let task: Task<Void, Never>

  init(_ channel: NIOAsyncChannel<WebSocketFrame, WebSocketFrame>) {
    let (inStream, received) = AsyncThrowingStream<SocketMessage, any Error>.makeStream()
    let (outStream, outbound) = AsyncStream<WebSocketFrame>.makeStream()
    self.inbound = inStream.makeAsyncIterator()
    self.outbound = outbound
    task = Task {
      do {
        try await channel.executeThenClose { frames, writer in
          try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
              for await frame in outStream { try await writer.write(frame) }
            }
            group.addTask {
              for try await frame in frames {
                switch frame.opcode {
                case .text: received.yield(.text(String(buffer: frame.unmaskedData)))
                case .binary: received.yield(.binary(Array(buffer: frame.unmaskedData)))
                case .ping:
                  outbound.yield(
                    WebSocketFrame(
                      fin: true, opcode: .pong, maskKey: .random(), data: frame.unmaskedData))
                case .connectionClose: return
                default: break
                }
              }
            }
            try await group.next()
            group.cancelAll()
          }
        }
        received.finish()
      } catch {
        received.finish(throwing: error)
      }
    }
  }

  func send(_ message: SocketMessage) async throws {
    var data = ByteBuffer()
    let opcode: WebSocketOpcode
    switch message {
    case .text(let s):
      data.writeString(s)
      opcode = .text
    case .binary(let b):
      data.writeBytes(b)
      opcode = .binary
    }
    outbound.yield(WebSocketFrame(fin: true, opcode: opcode, maskKey: .random(), data: data))
  }

  func receive() async throws -> SocketMessage? {
    try await inbound.next()
  }

  func close(code: Int, reason: String) async {
    var data = ByteBuffer()
    data.write(webSocketErrorCode: WebSocketErrorCode(codeNumber: code))
    data.writeString(reason)
    outbound.yield(
      WebSocketFrame(fin: true, opcode: .connectionClose, maskKey: .random(), data: data))
    outbound.finish()
    await task.value
  }
}
