#if canImport(os)
  import os
#elseif canImport(Glibc)
  import Glibc
#elseif canImport(Musl)
  import Musl
#endif

/// How much the client logs (`LogLevel` in `@rocicorp/logger`). A level
/// includes the levels after it: `.warn` logs warnings and errors.
public enum LogLevel: Int, Sendable, Comparable, CaseIterable, CustomStringConvertible {
  case debug, info, warn, error

  public static func < (a: Self, b: Self) -> Bool { a.rawValue < b.rawValue }

  public var description: String {
    switch self {
    case .debug: "debug"
    case .info: "info"
    case .warn: "warn"
    case .error: "error"
    }
  }
}

/// Where the client's log messages go (`LogSink` in `@rocicorp/logger`).
public protocol LogSink: Sendable {
  /// - Parameters:
  ///   - level: How severe the message is.
  ///   - message: The message, without the context.
  ///   - context: Which client logged the message, such as `clientID`.
  func log(level: LogLevel, message: String, context: [String: String])
}

/// The default sink: `os.Logger` (subsystem `zero`, category `client`) on
/// Apple platforms, standard error elsewhere.
public struct DefaultLogSink: LogSink {
  public init() {}

  public func log(level: LogLevel, message: String, context: [String: String]) {
    let prefix = context.sorted { $0.key < $1.key }.map { "\($0.key)=\($0.value)" }
      .joined(separator: " ")
    let line = prefix.isEmpty ? message : prefix + " " + message
    #if canImport(os)
      let logger = os.Logger(subsystem: "zero", category: "client")
      switch level {
      case .debug: logger.debug("\(line, privacy: .public)")
      case .info: logger.info("\(line, privacy: .public)")
      case .warn: logger.warning("\(line, privacy: .public)")
      case .error: logger.error("\(line, privacy: .public)")
      }
    #else
      var text = "\(level) \(line)\n"
      text.withUTF8 { buffer in _ = write(2, buffer.baseAddress, buffer.count) }
    #endif
  }
}

/// A sink with a level and a context (`LogContext`): the messages below the
/// level are dropped without being built.
struct Log: Sendable {
  let level: LogLevel
  let sink: any LogSink
  var context: [String: String] = [:]

  func with(_ key: String, _ value: String) -> Log {
    var copy = self
    copy.context[key] = value
    return copy
  }

  func callAsFunction(_ level: LogLevel, _ message: @autoclosure () -> String) {
    guard level >= self.level else { return }
    sink.log(level: level, message: message(), context: context)
  }

  func debug(_ message: @autoclosure () -> String) { self(.debug, message()) }
  func info(_ message: @autoclosure () -> String) { self(.info, message()) }
  func warn(_ message: @autoclosure () -> String) { self(.warn, message()) }
  func error(_ message: @autoclosure () -> String) { self(.error, message()) }
}
