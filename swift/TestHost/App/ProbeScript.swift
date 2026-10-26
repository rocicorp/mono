import Foundation

/// The transitions `FrameProbe` checks, driven by the app itself
/// (`-probe -probe-script chat|issues`), so they run the same with or
/// without UI tests. Each is a phase of the probe's summary.
@MainActor
enum ProbeScript {
  static var probe: FrameProbe { .shared }

  static func chat(
    latestID: () -> String?, toLatest: () -> Void, receive: () -> Void
  ) async {
    probe.setPhase("open")
    await until(10) { latestID().map { probe.visible.last == $0 } ?? false }
    await sleep(1)

    // Older pages of many heights load under each fling; at each rest the
    // space before the rows is given back; at the top, it's taken away.
    probe.setPhase("older")
    for _ in 0..<80 where !atTop("m0000") {
      await probe.drive(velocity: -2000)
      await sleep(0.6)
    }
    await sleep(1)

    probe.setPhase("latest")
    toLatest()
    await landed()

    // Far up, then Latest: with a message received while away, and without.
    for round in 0..<6 {
      let withReceive = round % 2 == 1
      probe.setPhase("far-up")
      for _ in 0..<10 {
        await probe.drive(velocity: -2500)
        await sleep(0.4)
      }
      await sleep(1)
      probe.setPhase(withReceive ? "receive-latest" : "latest")
      if withReceive {
        receive()
        await sleep(1)
      }
      toLatest()
      await landed()
    }
    probe.finish()
  }

  static func issues(growHeader: () -> Void) async {
    probe.setPhase("open")
    await until(10) { probe.visible.contains("i0000") }
    await sleep(1)

    // The header changes height: on screen, at the top, the rows go down
    // and up with it; above the screen, nothing on screen moves.
    probe.setPhase("header")
    for _ in 0..<5 {
      growHeader()
      await sleep(0.6)
    }
    await probe.drive(velocity: 1500)
    await sleep(0.6)
    for _ in 0..<5 {
      growHeader()
      await sleep(0.6)
    }

    probe.setPhase("down")
    for _ in 0..<60 where deepest() < 400 {
      await probe.drive(velocity: 2000)
      await sleep(0.5)
    }
    probe.setPhase("up")
    for _ in 0..<80 where !atStart() {
      await probe.drive(velocity: -2000)
      await sleep(0.5)
    }
    await sleep(1)

    #if os(macOS)
      // The selection moves down by the arrow key past the screen's
      // bottom, the list scrolling a row at a time, then back up.
      probe.setPhase("keys")
      for down in Array(repeating: true, count: 30) + Array(repeating: false, count: 12) {
        probe.beginJump()
        probe.pressArrow(down: down)
        await sleep(0.15)
      }
      await sleep(1)
    #endif
    probe.finish()
  }

  /// The list's start is at its top: the header, or the first row.
  static func atStart() -> Bool {
    atTop("header") || atTop("i0000")
  }

  static func atTop(_ id: String) -> Bool {
    probe.top(of: id).map { abs($0) <= 0.5 } ?? false
  }

  static func deepest() -> Int {
    probe.visible.compactMap { Int($0.dropFirst()) }.max() ?? 0
  }

  /// A jump's window settles before the next phase drives the list: until
  /// it has, the jump owns the position, and a drive would be undone. A
  /// stalled frame (a slow simulator's) can take the whole wait.
  static func landed() async {
    await sleep(1.5)
    await until(10) { !probe.jumping }
  }

  static func until(_ seconds: Double, _ done: () -> Bool) async {
    let end = Date().addingTimeInterval(seconds)
    while Date() < end, !done() { await sleep(0.05) }
  }

  static func sleep(_ seconds: Double) async {
    try? await Task.sleep(for: .seconds(seconds))
  }
}
