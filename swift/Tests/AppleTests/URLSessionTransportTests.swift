#if canImport(Network)
  import Foundation
  import Testing
  import ZeroTesting

  @testable import Zero

  /// The URLSession transport against a WebSocket server on the loopback
  /// interface.
  @Suite(.timeLimit(.minutes(1))) struct URLSessionTransportTests {
    @Test func sendsTheProtocolAndExchangesFrames() async throws {
      let server = try WebSocketServer()
      let port = try await server.start()
      defer { server.stop() }
      var events = server.events.makeAsyncIterator()

      let socket = try await URLSessionTransport().connect(
        url: "ws://127.0.0.1:\(port)/sync/v53/connect?clientID=c", secProtocol: "abc")
      guard case .connected(let peer, let protocols)? = await events.next() else {
        Issue.record("not connected")
        return
      }
      #expect(protocols == ["abc"])

      try await socket.send(.text("hello"))
      guard case .message(_, let message)? = await events.next() else {
        Issue.record("no message")
        return
      }
      #expect(message == .text("hello"))

      peer.send(.text("hi"))
      #expect(try await socket.receive() == .text("hi"))
      peer.send(.binary([1, 2, 3]))
      #expect(try await socket.receive() == .binary([1, 2, 3]))
      // Larger than URLSessionWebSocketTask's default limit of 1 MB.
      let large = String(repeating: "x", count: 3 << 20)
      peer.send(.text(large))
      #expect(try await socket.receive() == .text(large))

      peer.close(code: 4001, reason: "bye")
      await #expect(throws: SocketClosed(code: 4001, reason: "bye", wasClean: true)) {
        try await socket.receive()
      }
    }

    @Test func failsToConnectWhenNothingListens() async throws {
      let server = try WebSocketServer()
      let port = try await server.start()
      server.stop()
      try await Task.sleep(for: .milliseconds(100))
      await #expect(throws: SocketClosed.self) {
        try await URLSessionTransport().connect(url: "ws://127.0.0.1:\(port)/", secProtocol: nil)
      }
    }

    /// Records what a socket reports, on the main actor.
    @MainActor final class Recorder: SocketEvents {
      var opened = false
      var received: [SocketMessage] = []
      var closed: SocketClosed?
      func socketDidOpen() { opened = true }
      func socketDidReceive(_ message: SocketMessage) { received.append(message) }
      func socketDidClose(code: Int, reason: String, wasClean: Bool) {
        closed = SocketClosed(code: code, reason: reason, wasClean: wasClean)
      }
    }

    @Test @MainActor func keepsTheOrderOfFramesSentBeforeAndAfterTheOpen() async throws {
      let server = try WebSocketServer()
      let port = try await server.start()
      defer { server.stop() }
      var events = server.events.makeAsyncIterator()

      let recorder = Recorder()
      let factory = TransportSocketFactory(URLSessionTransport())
      let connection = factory.open(
        url: "ws://127.0.0.1:\(port)/", secProtocol: nil, events: recorder)
      for i in 0..<200 { connection.send("\(i)") }
      guard case .connected(let peer, _)? = await events.next() else {
        Issue.record("not connected")
        return
      }
      var texts: [String] = []
      while texts.count < 200, case .message(_, .text(let text))? = await events.next() {
        texts.append(text)
      }
      #expect(texts == (0..<200).map(String.init))
      #expect(recorder.opened)

      peer.close(code: 4003, reason: "done")
      while recorder.closed == nil { try await Task.sleep(for: .milliseconds(10)) }
      #expect(recorder.closed == SocketClosed(code: 4003, reason: "done", wasClean: true))
    }

    @Test @MainActor func connectsAClientThroughTheDefaultFactory() async throws {
      let server = try WebSocketServer()
      let port = try await server.start()
      defer { server.stop() }
      var events = server.events.makeAsyncIterator()

      var options = ZeroOptions(
        cacheURL: "http://127.0.0.1:\(port)", userID: "u", storageKey: UUID().uuidString,
        storage: .memory)
      options.logSink = RecordingLogSink()
      let zero = try Zero(schema: ZeroSchema(tables: []), options: options)
      defer { zero.close() }
      guard case .connected(let peer, let protocols)? = await events.next() else {
        Issue.record("not connected")
        return
      }
      let decoded = try SecProtocols.decode(#require(protocols.first))
      #expect(decoded["initConnectionMessage"]?[0] == "initConnection")

      peer.send(.text(#"["connected",{"wsid":"w"}]"#))
      while zero.connection.state != .connected { try await Task.sleep(for: .milliseconds(10)) }
    }
  }
#endif
