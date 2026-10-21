import SwiftUI
import ZeroUI

/// Opens the screen the `-screen` launch argument names (`chat`, `issues`),
/// or a menu of them.
@main
struct TestHostApp: App {
  @State private var backend = Backend()

  var body: some Scene {
    WindowGroup {
      Group {
        switch UserDefaults.standard.string(forKey: "screen") {
        case "chat": ChatScreen(backend: backend)
        case "issues": IssuesScreen(backend: backend)
        default:
          NavigationStack {
            List {
              NavigationLink("Chat") { ChatScreen(backend: backend) }
              NavigationLink("Issues") { IssuesScreen(backend: backend) }
            }
            .navigationTitle("Zero Test Host")
          }
        }
      }
      .zero(backend.zero)
    }
  }
}
