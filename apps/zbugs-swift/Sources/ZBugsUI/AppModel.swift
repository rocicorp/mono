#if canImport(SwiftUI)
  import Foundation
  import Observation
  import Security
  import ZBugs
  import Zero

  /// The app's state outside its views: where the servers are, who is
  /// logged in, and the client for them.
  @Observable
  @MainActor
  public final class AppModel {
    /// zero-cache.
    public private(set) var cacheURL: String
    /// The zbugs API server: login, and the queries and mutators zero-cache
    /// forwards.
    public private(set) var apiURL: String
    public private(set) var auth: AuthData?
    /// A new client whenever the servers or the user change, as zbugs'
    /// `ZeroProvider` makes one.
    public private(set) var zero: Zero<AuthData?>
    @ObservationIgnored private var token: String?
    private let defaults: UserDefaults

    public nonisolated static let projectName = zeroProjectName

    /// - Parameter environment: `ZBUGS_TOKEN`, `ZBUGS_CACHE_URL`, and
    ///   `ZBUGS_API_URL` override the saved token and servers without saving
    ///   them, for UI tests.
    public init(
      defaults: UserDefaults = .standard,
      environment: [String: String] = ProcessInfo.processInfo.environment
    ) {
      self.defaults = defaults
      let cacheURL =
        environment["ZBUGS_CACHE_URL"] ?? defaults.string(forKey: "cacheURL")
        ?? "http://127.0.0.1:4848"
      let apiURL =
        environment["ZBUGS_API_URL"] ?? defaults.string(forKey: "apiURL")
        ?? "http://127.0.0.1:5173"
      // An expired token logs out.
      let saved = environment["ZBUGS_TOKEN"] ?? Keychain.token
      let auth = saved.flatMap { AuthData(jwt: $0, now: Date().timeIntervalSince1970) }
      let token = auth == nil ? nil : saved
      self.cacheURL = cacheURL
      self.apiURL = apiURL
      self.token = token
      self.auth = auth
      zero = Self.makeZero(cacheURL: cacheURL, apiURL: apiURL, token: token, auth: auth)
      preload()
    }

    /// Logs in with a zbugs JWT, from the GitHub login or a dev token.
    public func logIn(token: String) throws {
      let token = token.trimmingCharacters(in: .whitespacesAndNewlines)
      guard let auth = AuthData(jwt: token, now: Date().timeIntervalSince1970) else {
        throw LoginError("That is not a current zbugs token.")
      }
      Keychain.token = token
      self.token = token
      self.auth = auth
      restart()
    }

    public func logOut() {
      Keychain.token = nil
      token = nil
      auth = nil
      restart()
    }

    public func setServers(cacheURL: String, apiURL: String) {
      defaults.set(cacheURL, forKey: "cacheURL")
      defaults.set(apiURL, forKey: "apiURL")
      self.cacheURL = cacheURL
      self.apiURL = apiURL
      restart()
    }

    /// Why the last mutation failed, for an alert; nil once seen.
    public var mutationError: String?

    /// Runs a mutator and reports its failure, on the client or the server.
    public func mutate(_ request: MutateRequest<AuthData?>) {
      let result = zero.mutate(request)
      if case .success(let outcome)? = result.client.result, let message = outcome.errorMessage {
        mutationError = message
        return
      }
      Task { [weak self] in
        if let message = try? await result.server.value.errorMessage {
          self?.mutationError = message
        }
      }
    }

    /// The URL that starts the GitHub login and ends at `callbackScheme`
    /// with the token.
    public var loginURL: URL? {
      var c = URLComponents(string: apiURL + "/api/login/github")
      c?.queryItems = [URLQueryItem(name: "redirect", value: "\(Self.callbackScheme)://auth")]
      return c?.url
    }

    public static let callbackScheme = "zbugs-swift"

    private func restart() {
      zero.close()
      zero = Self.makeZero(cacheURL: cacheURL, apiURL: apiURL, token: token, auth: auth)
      preload()
    }

    /// What zbugs preloads: the project's issues with their first comments,
    /// and the users, labels, and projects.
    private func preload() {
      let ttl = TTL.seconds(10)
      _ = zero.preload(
        Queries.issuePreloadV2(ProjectArgs(projectName: Self.projectName)), ttl: ttl)
      _ = zero.preload(Queries.allUsers(), ttl: ttl)
      _ = zero.preload(Queries.allLabels(), ttl: ttl)
      _ = zero.preload(Queries.allProjects(), ttl: ttl)
    }

    private static func makeZero(
      cacheURL: String, apiURL: String, token: String?, auth: AuthData?
    ) -> Zero<AuthData?> {
      var options = ZeroOptions(cacheURL: cacheURL, userID: auth?.sub, auth: token)
      options.mutateURL = apiURL + "/api/mutate"
      options.queryURL = apiURL + "/api/query"
      options.logLevel = .info
      do {
        return try Zero(schema: .zbugs, context: auth, mutators: Mutators.all, options: options)
      } catch {
        // Only a mutator registered twice fails; storage falls back to
        // memory on its own.
        preconditionFailure("zbugs' mutators: \(error)")
      }
    }
  }

  extension MutationOutcome {
    var errorMessage: String? {
      switch self {
      case .success: nil
      case .appError(let message, _), .zeroError(let message): message
      }
    }
  }

  public struct LoginError: Error, CustomStringConvertible {
    public let description: String
    init(_ description: String) { self.description = description }
  }

  /// The token in the keychain, so it outlives the app.
  enum Keychain {
    private static var query: [String: Any] {
      [
        kSecClass as String: kSecClassGenericPassword,
        kSecAttrService as String: "dev.rocicorp.zbugs",
        kSecAttrAccount as String: "jwt",
      ]
    }

    static var token: String? {
      get {
        var q = query
        q[kSecReturnData as String] = true
        var item: CFTypeRef?
        guard SecItemCopyMatching(q as CFDictionary, &item) == errSecSuccess,
          let data = item as? Data
        else { return nil }
        return String(data: data, encoding: .utf8)
      }
      set {
        SecItemDelete(query as CFDictionary)
        guard let newValue else { return }
        var q = query
        q[kSecValueData as String] = Data(newValue.utf8)
        SecItemAdd(q as CFDictionary, nil)
      }
    }
  }
#endif
