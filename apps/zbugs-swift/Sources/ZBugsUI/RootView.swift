#if canImport(SwiftUI)
  import SwiftUI
  import ZBugs
  import Zero
  import ZeroUI
  import ZeroVirtual

  /// Which UI layer draws the issue list.
  public enum ListLayer: String, CaseIterable, Identifiable, Sendable {
    case swiftUI = "SwiftUI"
    #if canImport(UIKit)
      case native = "UIKit"
    #else
      case native = "AppKit"
    #endif
    public var id: Self { self }
  }

  /// The app: filters, the issue list, and the selected issue.
  ///
  /// The app makes the model once and passes it in: SwiftUI runs a view's
  /// `@State` initializer each time it re-creates the view, and each model
  /// starts a client.
  public struct RootView: View {
    let model: AppModel
    @State private var filter = IssueFilter()
    @State private var selection: String?
    /// The issue list's rows, for the client.
    @State private var issues: IssueVirtualizer?
    /// The selected issue and where the list was, kept with the window: a
    /// relaunch opens the list at the issue, or else where it was.
    @SceneStorage("selection") private var savedSelection: String?
    @SceneStorage("issueList") private var savedScroll: Data?
    @State private var saving = Debouncer()
    @State private var listLayer = ListLayer.swiftUI
    @State private var sheet: Sheet?
    /// All three columns, on an iPad or a Mac: the filters are how you get
    /// around.
    @State private var columns = NavigationSplitViewVisibility.all
    /// The column an iPhone shows: the list, with the filters before it and
    /// the selected issue after. It starts at the filters and moves to the
    /// list as the view appears: a list that has rows before its navigation
    /// bar is set up (from the local store, offline) opens with its large
    /// title collapsed.
    @State private var compactColumn = NavigationSplitViewColumn.sidebar
    #if os(iOS)
      @Environment(\.horizontalSizeClass) private var sizeClass
    #endif

    enum Sheet: String, Identifiable {
      case newIssue, login, servers
      var id: Self { self }
    }

    public init(model: AppModel) {
      self.model = model
    }

    public var body: some View {
      NavigationSplitView(columnVisibility: $columns, preferredCompactColumn: $compactColumn) {
        Sidebar(filter: $filter, listLayer: $listLayer)
          .navigationTitle("Zero Bugs")
          #if os(macOS)
            // Wide enough for the pickers' values.
            .navigationSplitViewColumnWidth(min: 220, ideal: 240)
          #endif
          #if os(iOS)
            .toolbar {
              // The columns don't report the window's size class.
              if sizeClass == .compact {
                ToolbarItem(placement: .confirmationAction) {
                  Button("Issues") { compactColumn = .content }
                }
              }
            }
          #endif
      } content: {
        list
          .navigationTitle(filter.status.title + " Issues")
          .searchable(text: $filter.text, prompt: "Search issues")
          #if os(macOS)
            // After `searchable`, which drops a width set before it.
            .navigationSplitViewColumnWidth(min: 280, ideal: 340)
          #endif
          .toolbar { toolbar }
      } detail: {
        if let selection {
          IssueDetail(id: selection, zero: model.zero)
            // Its comments' virtualizer is of the client.
            .id([selection, String(describing: ObjectIdentifier(model.zero))])
        } else {
          ContentUnavailableView("No Issue Selected", systemImage: "list.bullet.rectangle")
        }
      }
      .onAppear {
        compactColumn = .content
        if selection == nil { selection = savedSelection }
      }
      // A new client, as the user logs in: the list starts over, at the
      // selected issue.
      .onChange(of: ObjectIdentifier(model.zero), initial: true) { makeIssues() }
      // Another list: from its top, not the issue it opened at.
      .onChange(of: filter) { _, filter in
        issues?.permalinkID = nil
        issues?.listContext = filter.listContext
      }
      // A new issue is selected outside a List, which a collapsed split
      // view doesn't follow on its own.
      .onChange(of: selection) { _, selection in
        if selection != nil { compactColumn = .detail }
        savedSelection = selection
      }
      .zero(model.zero)
      .environment(model)
      .sheet(item: $sheet) { sheet in
        Group {
          switch sheet {
          case .newIssue: NewIssue { selection = $0 }
          case .login: LoginSheet()
          case .servers: ServerSettings()
          }
        }
        .environment(model)
        .zero(model.zero)
      }
      .alert(
        "Couldn't Save",
        isPresented: Binding {
          model.mutationError != nil
        } set: { _ in
          model.mutationError = nil
        }
      ) {
      } message: {
        Text(model.mutationError ?? "")
      }
    }

    @ViewBuilder private var list: some View {
      if let issues {
        switch listLayer {
        case .swiftUI:
          IssueList(list: issues, selection: listSelection)
        case .native:
          NativeIssueList(list: issues, selection: highlighted) { open($0) }
            // A view of another virtualizer, for another client.
            .id(ObjectIdentifier(issues))
        }
      }
    }

    /// The list's selection: the highlighted issue, which a tap or a key
    /// opens.
    private var listSelection: Binding<String?> {
      Binding {
        highlighted
      } set: { id in
        if let id { open(id) }
      }
    }

    private func open(_ id: String) {
      selection = id
      // Also when it's the issue the detail shows, which doesn't change
      // the selection.
      compactColumn = .detail
    }

    /// The list for the client, opened at the selected issue, as the web
    /// app's list permalink opens it, or else where it was.
    private func makeIssues() {
      let permalinkID = selection ?? savedSelection
      let scroll =
        permalinkID == nil
        ? savedScroll.flatMap { try? JSONDecoder().decode(IssueScrollState.self, from: $0) } : nil
      let list = IssueVirtualizer.issues(
        model.zero, filter: filter, permalinkID: permalinkID, scrollState: scroll)
      let save = $savedScroll
      list.onScrollStateChange = { [saving] state in
        saving.run { save.wrappedValue = try? JSONEncoder().encode(state) }
      }
      issues = list
    }

    /// The issue the list highlights: the selected one, when the list is
    /// beside it (not on an iPhone). The columns themselves are compact on an
    /// iPad too.
    private var highlighted: String? {
      #if os(iOS)
        sizeClass == .compact ? nil : selection
      #else
        selection
      #endif
    }

    @ToolbarContentBuilder private var toolbar: some ToolbarContent {
      ToolbarItem {
        ConnectionBadge(connection: model.zero.connection)
      }
      ToolbarItem {
        Button("New Issue", systemImage: "square.and.pencil") { sheet = .newIssue }
          .disabled(model.auth == nil)
      }
      ToolbarItem {
        Menu("Account", systemImage: "person.crop.circle") {
          if let auth = model.auth {
            Text("Logged in as \(auth.sub) (\(auth.role.rawValue))")
            Button("Log Out") { model.logOut() }
          } else {
            Button("Log In…") { sheet = .login }
          }
          Button("Servers…") { sheet = .servers }
        }
      }
    }
  }

  private struct Sidebar: View {
    @Binding var filter: IssueFilter
    @Binding var listLayer: ListLayer
    @Environment(AppModel.self) private var model

    var body: some View {
      Form {
        Picker("Status", selection: $filter.status) {
          ForEach(IssueStatus.allCases) { Text($0.title).tag($0) }
        }
        Picker("Sort", selection: $filter.sortField) {
          Text("Modified").tag(SortField.modified)
          Text("Created").tag(SortField.created)
        }
        Picker("Order", selection: $filter.sortDirection) {
          Text("Newest First").tag(AST.Direction.desc)
          Text("Oldest First").tag(AST.Direction.asc)
        }
        if let auth = model.auth {
          MineToggles(filter: $filter, userID: auth.sub)
        }
        Picker("List", selection: $listLayer) {
          ForEach(ListLayer.allCases) { Text($0.rawValue).tag($0) }
        }
      }
      .formStyle(.grouped)
    }
  }

  /// Filters by the logged-in user, whose login the list matches.
  private struct MineToggles: View {
    @Binding var filter: IssueFilter
    @ZeroQuery<User, User?, AuthData?> var user: User?

    init(filter: Binding<IssueFilter>, userID: String) {
      _filter = filter
      _user = ZeroQuery(Queries.user(userID))
    }

    var body: some View {
      if let login = user?.login {
        Toggle(
          "Created by Me",
          isOn: Binding {
            filter.creator == login
          } set: {
            filter.creator = $0 ? login : nil
          })
        Toggle(
          "Assigned to Me",
          isOn: Binding {
            filter.assignee == login
          } set: {
            filter.assignee = $0 ? login : nil
          })
      }
    }
  }

  /// Runs the last of the actions it's given half a second after it, as
  /// the list reports where it is on every scroll.
  @MainActor
  final class Debouncer {
    private var task: Task<Void, Never>?

    func run(_ action: @escaping @MainActor () -> Void) {
      task?.cancel()
      task = Task {
        try? await Task.sleep(for: .milliseconds(500))
        if !Task.isCancelled { action() }
      }
    }
  }

  /// The connection state, as a dot.
  private struct ConnectionBadge: View {
    let connection: Connection

    var body: some View {
      let (color, text): (Color, String) =
        switch connection.state {
        case .connected: (.green, "Connected")
        case .connecting, .initializing: (.yellow, "Connecting")
        case .needsAuth: (.orange, "Log In Again")
        case .disconnected, .closed: (.red, "Offline")
        case .error: (.red, "Error")
        }
      SwiftUI.Label {
        Text(text)
      } icon: {
        Image(systemName: "circle.fill").foregroundStyle(color)
      }
      #if os(iOS)
        // The text doesn't fit in an iPhone's navigation bar.
        .labelStyle(.iconOnly)
        .accessibilityLabel(text)
      #else
        .labelStyle(.titleAndIcon)
      #endif
      .font(.caption)
      .help(String(describing: connection.state))
    }
  }
#endif
