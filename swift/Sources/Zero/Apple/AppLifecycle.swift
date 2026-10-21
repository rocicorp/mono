#if canImport(UIKit) && !os(watchOS)
  import UIKit
#elseif canImport(AppKit)
  import AppKit
#endif
#if canImport(Network)
  import Network
#endif
#if canImport(Darwin)
  import Foundation
#endif

/// What the app's lifecycle does to a client.
@MainActor
protocol LifecycleClient: AnyObject {
  func setVisible(_ visible: Bool)
  func reconnectNow()
  func whenPendingSettled(_ done: @escaping () -> Void)
  func flushStorage()
}

#if canImport(Darwin)
  /// Connects a client to the app's lifecycle and the network:
  ///
  /// - iOS: in the background, the client pushes its pending mutations
  ///   within a short background task, then disconnects; it reconnects in the
  ///   foreground.
  /// - macOS: the client disconnects when the Mac sleeps and reconnects when
  ///   it wakes.
  /// - Both: when the network is back, the client reconnects without waiting
  ///   for its backoff, and checks an open connection when the network
  ///   changes.
  @MainActor
  final class AppLifecycle {
    /// How long the app waits in the background for the server to confirm
    /// the pending mutations.
    static let backgroundFlushTimeout: Duration = .seconds(10)

    private weak var client: (any LifecycleClient)?
    private var observers: [any NSObjectProtocol] = []
    #if canImport(UIKit) && !os(watchOS)
      private var flush: BackgroundFlush?
    #endif
    #if canImport(Network)
      private let monitor = NWPathMonitor()
      private var lastPath: (status: NWPath.Status, interfaces: [String])?
    #endif

    init(_ client: any LifecycleClient) {
      self.client = client
      #if canImport(UIKit) && !os(watchOS)
        let center = NotificationCenter.default
        observers.append(
          center.addObserver(
            forName: UIApplication.didEnterBackgroundNotification, object: nil, queue: .main
          ) { [weak self] _ in MainActor.assumeIsolated { self?.didEnterBackground() } })
        observers.append(
          center.addObserver(
            forName: UIApplication.willEnterForegroundNotification, object: nil, queue: .main
          ) { [weak self] _ in MainActor.assumeIsolated { self?.willEnterForeground() } })
        if UIApplication.shared.applicationState == .background {
          client.setVisible(false)
        }
      #elseif canImport(AppKit)
        let center = NSWorkspace.shared.notificationCenter
        observers.append(
          center.addObserver(forName: NSWorkspace.willSleepNotification, object: nil, queue: .main)
          { [weak self] _ in
            MainActor.assumeIsolated {
              self?.client?.setVisible(false)
              self?.client?.flushStorage()
            }
          })
        observers.append(
          center.addObserver(forName: NSWorkspace.didWakeNotification, object: nil, queue: .main) {
            [weak self] _ in MainActor.assumeIsolated { self?.client?.setVisible(true) }
          })
      #endif
      #if canImport(Network)
        monitor.pathUpdateHandler = { [weak self] path in
          let status = path.status
          let interfaces = path.availableInterfaces.map(\.name)
          Task { @MainActor in self?.pathChanged(status, interfaces) }
        }
        monitor.start(queue: DispatchQueue(label: "zero.network"))
      #endif
    }

    isolated deinit {
      stop()
    }

    func stop() {
      #if canImport(UIKit) && !os(watchOS)
        for o in observers { NotificationCenter.default.removeObserver(o) }
      #elseif canImport(AppKit)
        for o in observers { NSWorkspace.shared.notificationCenter.removeObserver(o) }
      #endif
      observers = []
      #if canImport(Network)
        monitor.cancel()
      #endif
    }

    #if canImport(Network)
      private func pathChanged(_ status: NWPath.Status, _ interfaces: [String]) {
        defer { lastPath = (status, interfaces) }
        // The first report is the network as it is at launch.
        guard let last = lastPath, status == .satisfied else { return }
        if last.status != .satisfied || last.interfaces != interfaces {
          client?.reconnectNow()
        }
      }
    #endif

    #if canImport(UIKit) && !os(watchOS)
      /// Pushes the pending mutations, then disconnects, within a background
      /// task.
      private func didEnterBackground() {
        guard let client else { return }
        flush?.end()
        let flush = BackgroundFlush(client)
        self.flush = flush
        flush.task = UIApplication.shared.beginBackgroundTask(
          withName: "Zero: push pending mutations"
        ) { MainActor.assumeIsolated { flush.finish() } }
        client.whenPendingSettled { flush.finish() }
        Task { @MainActor in
          try? await Task.sleep(for: Self.backgroundFlushTimeout)
          flush.finish()
        }
      }

      private func willEnterForeground() {
        flush?.end()
        flush = nil
        client?.setVisible(true)
      }
    #endif
  }

  #if canImport(UIKit) && !os(watchOS)
    /// One trip to the background: ends at the first of the server
    /// confirming the pending mutations, the timeout, and the system
    /// expiring the background task.
    @MainActor
    private final class BackgroundFlush {
      weak var client: (any LifecycleClient)?
      var task = UIBackgroundTaskIdentifier.invalid
      private var done = false

      init(_ client: any LifecycleClient) { self.client = client }

      /// Disconnects and ends the background task.
      func finish() {
        guard !done else { return }
        client?.setVisible(false)
        client?.flushStorage()
        end()
      }

      /// Ends the background task, for example when the app is back.
      func end() {
        done = true
        if task != .invalid { UIApplication.shared.endBackgroundTask(task) }
        task = .invalid
      }
    }
  #endif
#endif
