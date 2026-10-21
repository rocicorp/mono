/// The clock and timers the engine runs on: `Date.now()`, `performance.now()`
/// and `setTimeout` in JS. Tests swap in a manual scheduler so a scenario
/// runs the same way every time.
@MainActor
public protocol Scheduler: AnyObject {
  /// Milliseconds since 1970 (`Date.now()`).
  var now: Double { get }
  /// Milliseconds on a monotonic clock (`performance.now()`).
  var monotonicNow: Double { get }
  /// Calls `action` on the main actor after `ms` milliseconds.
  func schedule(after ms: Double, _ action: @escaping @MainActor () -> Void) -> any ScheduledTimer
  /// Calls `action` every `ms` milliseconds until cancelled
  /// (`setInterval`).
  func scheduleRepeating(
    every ms: Double, _ action: @escaping @MainActor () -> Void
  )
    -> any ScheduledTimer
  /// Calls `action` once the caller's synchronous code is done, before any
  /// timer: where JS runs microtasks. The engine queues its follow-ups to
  /// API calls here. By default a main-actor task.
  func checkpoint(_ action: @escaping @MainActor () -> Void)
}

extension Scheduler {
  public func checkpoint(_ action: @escaping @MainActor () -> Void) {
    Task { @MainActor in action() }
  }
}

/// A timer from `Scheduler.schedule`.
@MainActor
public protocol ScheduledTimer: AnyObject {
  /// Stops the timer from firing. Does nothing once it has fired.
  func cancel()
}

/// The real clock, with timers on the main actor.
@MainActor
public final class SystemScheduler: Scheduler {
  private let start = ContinuousClock.now

  public init() {}

  public var now: Double { wallClockMilliseconds() }

  public var monotonicNow: Double { Self.milliseconds(ContinuousClock.now - start) }

  public func schedule(
    after ms: Double, _ action: @escaping @MainActor () -> Void
  )
    -> any ScheduledTimer
  {
    let timer = SystemTimer()
    timer.task = Task { @MainActor in
      try? await Task.sleep(for: .milliseconds(max(0, ms)))
      guard !timer.cancelled else { return }
      timer.cancelled = true
      action()
    }
    return timer
  }

  public func scheduleRepeating(
    every ms: Double, _ action: @escaping @MainActor () -> Void
  )
    -> any ScheduledTimer
  {
    let timer = SystemTimer()
    timer.task = Task { @MainActor in
      while !timer.cancelled {
        try? await Task.sleep(for: .milliseconds(max(1, ms)))
        if timer.cancelled { return }
        action()
      }
    }
    return timer
  }

  static func milliseconds(_ d: Duration) -> Double {
    let (s, attoseconds) = d.components
    return Double(s) * 1000 + Double(attoseconds) / 1e15
  }
}

@MainActor
final class SystemTimer: ScheduledTimer {
  var task: Task<Void, Never>?
  var cancelled = false

  func cancel() {
    cancelled = true
    task?.cancel()
  }
}

#if canImport(Darwin)
  import Darwin
#elseif canImport(Glibc)
  import Glibc
#elseif canImport(Musl)
  import Musl
#endif

func wallClockMilliseconds() -> Double {
  var ts = timespec()
  clock_gettime(CLOCK_REALTIME, &ts)
  return Double(ts.tv_sec) * 1000 + Double(ts.tv_nsec) / 1e6
}

/// A value that arrives later, like a JS promise: the first `resolve` or
/// `reject` wins. Awaiting `value` suspends until then; `result` reads it
/// without waiting.
@MainActor
public final class Deferred<Value: Sendable> {
  public private(set) var result: Result<Value, any Error>?
  private var waiters: [CheckedContinuation<Value, any Error>] = []
  private var observers: [(Result<Value, any Error>) -> Void] = []

  public init() {}

  init(resolved value: Value) { result = .success(value) }

  public var value: Value {
    get async throws {
      if let result { return try result.get() }
      return try await withCheckedThrowingContinuation { waiters.append($0) }
    }
  }

  public var isSettled: Bool { result != nil }

  func resolve(_ value: Value) { settle(.success(value)) }
  func reject(_ error: any Error) { settle(.failure(error)) }

  /// Calls `observer` once settled (right away if it already is).
  func onSettle(_ observer: @escaping (Result<Value, any Error>) -> Void) {
    if let result { observer(result) } else { observers.append(observer) }
  }

  private func settle(_ r: Result<Value, any Error>) {
    guard result == nil else { return }
    result = r
    let waiters = self.waiters
    let observers = self.observers
    self.waiters = []
    self.observers = []
    for w in waiters { w.resume(with: r) }
    for o in observers { o(r) }
  }
}
