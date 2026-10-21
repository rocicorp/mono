#if canImport(SwiftUI)
  import AuthenticationServices
  import Foundation
  import SwiftUI
  import ZBugs
  import Zero
  import ZeroUI

  /// Files a new issue in the project.
  struct NewIssue: View {
    @Environment(AppModel.self) private var model
    @Environment(\.dismiss) private var dismiss
    @ZeroQuery(Queries.allProjects()) private var projects: [Project]
    @State private var title = ""
    @State private var description = ""
    private let created: (String) -> Void

    /// `created` gets the new issue's ID.
    init(created: @escaping (String) -> Void = { _ in }) {
      self.created = created
    }

    var body: some View {
      NavigationStack {
        Form {
          TextField("Title", text: $title)
          TextField("Description", text: $description, axis: .vertical)
            .lineLimit(5...20)
        }
        .formStyle(.grouped)
        .navigationTitle("New Issue")
        .toolbar {
          ToolbarItem(placement: .cancellationAction) {
            Button("Cancel") { dismiss() }
          }
          ToolbarItem(placement: .confirmationAction) {
            Button("Create") {
              guard let project else { return }
              let id = makeID()
              let time = now()
              model.mutate(
                Mutators.createIssue(
                  CreateIssueArgs(
                    id: id, title: title, description: description, created: time,
                    modified: time, projectID: project.id)))
              created(id)
              dismiss()
            }
            .disabled(title.isEmpty || project == nil)
          }
        }
      }
      // A sheet on iOS is the screen's width.
      #if os(macOS)
        .frame(minWidth: 420, minHeight: 300)
      #endif
    }

    private var project: Project? {
      projects.first { $0.lowerCaseName == AppModel.projectName.lowercased() }
    }
  }

  /// Logs in through GitHub, or with a token from `pnpm dev-token` in
  /// apps/zbugs.
  struct LoginSheet: View {
    @Environment(AppModel.self) private var model
    @Environment(\.dismiss) private var dismiss
    @Environment(\.webAuthenticationSession) private var webAuthenticationSession
    @State private var token = ""
    @State private var error: String?

    var body: some View {
      NavigationStack {
        Form {
          Section {
            Button("Log In with GitHub", systemImage: "person.badge.key") {
              Task { await logInWithGitHub() }
            }
          } footer: {
            Text("Through the zbugs API server at \(model.apiURL).")
          }
          Section("Development") {
            SecureField("Token", text: $token)
            Button("Log In with Token") { logIn(token) }
              .disabled(token.isEmpty)
          }
          if let error {
            Text(error).foregroundStyle(.red)
          }
        }
        .formStyle(.grouped)
        .navigationTitle("Log In")
        .toolbar {
          ToolbarItem(placement: .cancellationAction) {
            Button("Cancel") { dismiss() }
          }
        }
      }
      // A sheet on iOS is the screen's width.
      #if os(macOS)
        .frame(minWidth: 420, minHeight: 320)
      #endif
    }

    private func logInWithGitHub() async {
      guard let url = model.loginURL else {
        error = "The API server URL is not valid."
        return
      }
      do {
        let callback = try await webAuthenticationSession.authenticate(
          using: url, callbackURLScheme: AppModel.callbackScheme)
        let jwt = URLComponents(url: callback, resolvingAgainstBaseURL: false)?
          .queryItems?.first { $0.name == "jwt" }?.value
        logIn(jwt ?? "")
      } catch ASWebAuthenticationSessionError.canceledLogin {
        // The user closed the sheet.
      } catch {
        self.error = error.localizedDescription
      }
    }

    private func logIn(_ token: String) {
      do {
        try model.logIn(token: token)
        dismiss()
      } catch {
        self.error = String(describing: error)
      }
    }
  }

  /// Where the servers are.
  struct ServerSettings: View {
    @Environment(AppModel.self) private var model
    @Environment(\.dismiss) private var dismiss
    @State private var cacheURL = ""
    @State private var apiURL = ""

    var body: some View {
      NavigationStack {
        Form {
          TextField("zero-cache", text: $cacheURL)
          TextField("zbugs API", text: $apiURL)
        }
        .formStyle(.grouped)
        .autocorrectionDisabled()
        .navigationTitle("Servers")
        .toolbar {
          ToolbarItem(placement: .cancellationAction) {
            Button("Cancel") { dismiss() }
          }
          ToolbarItem(placement: .confirmationAction) {
            Button("Save") {
              model.setServers(cacheURL: cacheURL, apiURL: apiURL)
              dismiss()
            }
          }
        }
        .onAppear {
          cacheURL = model.cacheURL
          apiURL = model.apiURL
        }
      }
      // A sheet on iOS is the screen's width.
      #if os(macOS)
        .frame(minWidth: 420, minHeight: 220)
      #endif
    }
  }
#endif
