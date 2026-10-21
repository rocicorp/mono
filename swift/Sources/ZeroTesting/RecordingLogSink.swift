import Synchronization

/// A log sink that keeps the messages, for tests to check or to keep a test
/// run's output quiet.
public final class RecordingLogSink: LogSink {
  public struct Entry: Sendable, Hashable {
    public let level: LogLevel
    public let message: String
  }

  private let recorded = Mutex<[Entry]>([])

  public init() {}

  public func log(level: LogLevel, message: String, context: [String: String]) {
    recorded.withLock { $0.append(Entry(level: level, message: message)) }
  }

  public var entries: [Entry] { recorded.withLock { $0 } }
}
