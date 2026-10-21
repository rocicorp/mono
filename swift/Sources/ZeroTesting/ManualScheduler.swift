import Zero

/// A clock that only moves when told to, like fake timers in JS tests.
///
/// Timers run in order of due time, then of creation; a repeating timer
/// keeps its place among timers due at the same time (`setInterval` keeps
/// its timer ID).
@MainActor
public final class ManualScheduler: Scheduler {
  /// `Date.now()`.
  public private(set) var now: Double
  /// `performance.now()`: milliseconds since the scheduler was made.
  public private(set) var monotonicNow: Double = 0

  private final class Timer: ScheduledTimer {
    let sequence: Int
    var due: Double
    let interval: Double?
    let action: @MainActor () -> Void
    var cancelled = false

    init(sequence: Int, due: Double, interval: Double?, action: @escaping @MainActor () -> Void) {
      self.sequence = sequence
      self.due = due
      self.interval = interval
      self.action = action
    }

    func cancel() { cancelled = true }
  }

  private var timers: [Timer] = []
  private var checkpoints: [@MainActor () -> Void] = []
  private var nextSequence = 0

  public init(now: Double = 0) {
    self.now = now
  }

  public func schedule(
    after ms: Double, _ action: @escaping @MainActor () -> Void
  )
    -> any ScheduledTimer
  {
    add(due: monotonicNow + max(0, ms), interval: nil, action)
  }

  public func scheduleRepeating(
    every ms: Double, _ action: @escaping @MainActor () -> Void
  )
    -> any ScheduledTimer
  {
    add(due: monotonicNow + max(1, ms), interval: max(1, ms), action)
  }

  private func add(
    due: Double, interval: Double?, _ action: @escaping @MainActor () -> Void
  )
    -> Timer
  {
    nextSequence += 1
    let timer = Timer(sequence: nextSequence, due: due, interval: interval, action: action)
    timers.append(timer)
    return timer
  }

  /// Runs `action` before the next timer fires, or when the caller next
  /// suspends, whichever comes first.
  public func checkpoint(_ action: @escaping @MainActor () -> Void) {
    checkpoints.append(action)
    Task { @MainActor in self.runCheckpoints() }
  }

  /// Runs the pending checkpoints, as JS runs microtasks before a timer.
  public func runCheckpoints() {
    while !checkpoints.isEmpty {
      checkpoints.removeFirst()()
    }
  }

  /// The number of timers that have not fired or been cancelled.
  public var pendingTimers: Int { timers.count { !$0.cancelled } }

  private func nextDue(by limit: Double) -> Timer? {
    runCheckpoints()
    timers.removeAll { $0.cancelled }
    var best: Timer?
    for t in timers where t.due <= limit {
      if let b = best, (b.due, b.sequence) <= (t.due, t.sequence) { continue }
      best = t
    }
    return best
  }

  private func fire(_ timer: Timer) {
    let elapsed = timer.due - monotonicNow
    if elapsed > 0 {
      monotonicNow = timer.due
      now += elapsed
    }
    if let interval = timer.interval {
      timer.due += interval
    } else {
      timer.cancelled = true
    }
    timer.action()
  }

  /// Moves the clock forward by `ms`, firing every timer that comes due on
  /// the way, including ones those timers schedule.
  public func advance(by ms: Double) {
    let target = monotonicNow + ms
    while let timer = nextDue(by: target) { fire(timer) }
    now += target - monotonicNow
    monotonicNow = target
  }

  /// Fires the timers due now, and the zero-delay timers they schedule,
  /// until none is left.
  public func runDueTimers(limit: Int = 100_000) {
    var count = 0
    while let timer = nextDue(by: monotonicNow) {
      fire(timer)
      count += 1
      precondition(count < limit, "Timers keep scheduling timers")
    }
  }
}
