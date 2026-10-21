#if canImport(Darwin) || ZERO_URLSESSION
  import Foundation
  #if canImport(FoundationNetworking)
    import FoundationNetworking
  #endif
  import Synchronization

  /// A `Transport` over `URLSessionWebSocketTask`: the client's socket on
  /// Apple platforms.
  public final class URLSessionTransport: Transport {
    private let session: URLSession
    private let maximumMessageSize: Int

    /// - Parameters:
    ///   - configuration: The session's configuration, for example to add
    ///     headers or to route through a proxy.
    ///   - maximumMessageSize: The largest frame the socket accepts. A poke
    ///     can be larger than `URLSessionWebSocketTask`'s 1 MB default.
    public init(
      configuration: URLSessionConfiguration = .default, maximumMessageSize: Int = 64 << 20
    ) {
      session = URLSession(configuration: configuration)
      self.maximumMessageSize = maximumMessageSize
    }

    deinit {
      session.finishTasksAndInvalidate()
    }

    public func connect(url: String, secProtocol: String?) async throws -> any Socket {
      guard let target = URL(string: url) else {
        throw SocketClosed(code: 1006, reason: "Invalid URL: \(url)", wasClean: false)
      }
      let task = session.webSocketTask(with: target, protocols: secProtocol.map { [$0] } ?? [])
      task.maximumMessageSize = maximumMessageSize
      let socket = URLSessionSocket(task)
      try await socket.open()
      return socket
    }
  }

  /// One `URLSessionWebSocketTask`. Its delegate reports the open and the
  /// close.
  final class URLSessionSocket: NSObject, Socket, URLSessionWebSocketDelegate {
    private struct State {
      var opened: CheckedContinuation<Void, any Error>?
      var close: SocketClosed?
    }

    let task: URLSessionWebSocketTask
    private let state = Mutex(State())

    init(_ task: URLSessionWebSocketTask) {
      self.task = task
    }

    /// Starts the task and waits for the handshake.
    func open() async throws {
      try await withTaskCancellationHandler {
        try await withCheckedThrowingContinuation { (c: CheckedContinuation<Void, any Error>) in
          state.withLock { $0.opened = c }
          task.delegate = self
          task.resume()
        }
      } onCancel: {
        task.cancel()
      }
    }

    private func resumeOpen(_ result: Result<Void, any Error>) {
      let c = state.withLock { s in
        defer { s.opened = nil }
        return s.opened
      }
      c?.resume(with: result)
    }

    func urlSession(
      _ session: URLSession, webSocketTask: URLSessionWebSocketTask,
      didOpenWithProtocol protocol: String?
    ) {
      resumeOpen(.success(()))
    }

    func urlSession(
      _ session: URLSession, webSocketTask: URLSessionWebSocketTask,
      didCloseWith closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?
    ) {
      let close = SocketClosed(
        code: closeCode.rawValue, reason: reason.map { String(decoding: $0, as: UTF8.self) } ?? "",
        wasClean: true)
      state.withLock { $0.close = close }
      resumeOpen(.failure(close))
    }

    func urlSession(
      _ session: URLSession, task: URLSessionTask, didCompleteWithError error: (any Error)?
    ) {
      let close = state.withLock { s in
        if s.close == nil {
          s.close = SocketClosed(
            code: 1006, reason: error.map { $0.localizedDescription } ?? "", wasClean: false)
        }
        return s.close!
      }
      resumeOpen(.failure(close))
    }

    func send(_ message: SocketMessage) async throws {
      switch message {
      case .text(let text): try await task.send(.string(text))
      case .binary(let bytes): try await task.send(.data(Data(bytes)))
      }
    }

    func receive() async throws -> SocketMessage? {
      do {
        switch try await task.receive() {
        case .string(let text): return .text(text)
        case .data(let data): return .binary([UInt8](data))
        @unknown default: return nil
        }
      } catch {
        throw closeReason(error)
      }
    }

    /// How the socket closed, once `receive` fails: the close frame when one
    /// came, otherwise an abrupt close.
    private func closeReason(_ error: any Error) -> SocketClosed {
      if let close = state.withLock({ $0.close }) { return close }
      if task.closeCode != .invalid {
        return SocketClosed(
          code: task.closeCode.rawValue,
          reason: task.closeReason.map { String(decoding: $0, as: UTF8.self) } ?? "",
          wasClean: true)
      }
      return SocketClosed(code: 1006, reason: error.localizedDescription, wasClean: false)
    }

    func close(code: Int, reason: String) async {
      task.cancel(
        with: URLSessionWebSocketTask.CloseCode(rawValue: code) ?? .normalClosure,
        reason: reason.isEmpty ? nil : Data(reason.utf8))
    }
  }
#endif
