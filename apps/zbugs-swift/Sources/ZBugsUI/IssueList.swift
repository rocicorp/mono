#if canImport(SwiftUI)
  import Foundation
  import SwiftUI
  import ZBugs
  import Zero
  import ZeroUI
  import ZeroVirtual

  public enum IssueStatus: String, CaseIterable, Identifiable, Sendable {
    case open, closed, all
    public var id: Self { self }
    var title: String { rawValue.capitalized }
  }

  /// What the issue list shows, as zbugs' list page reads it from the URL.
  public struct IssueFilter: Hashable, Sendable {
    public var status = IssueStatus.open
    public var text = ""
    public var sortField = SortField.modified
    public var sortDirection = AST.Direction.desc
    public var creator: String?
    public var assignee: String?

    public init() {}

    var listContext: ListContextParams {
      ListContextParams(
        open: status == .open ? true : status == .closed ? false : nil,
        projectName: AppModel.projectName, assignee: assignee, creator: creator, labels: [],
        textFilter: text.isEmpty ? nil : text, sortField: sortField,
        sortDirection: sortDirection)
    }
  }

  /// The issue list's rows, paged in as it scrolls.
  typealias IssueVirtualizer = ZeroVirtualizer<
    AuthData?, Issue, String, IssueRowSort, ListContextParams
  >

  /// Where the issue list was, to put it back.
  typealias IssueScrollState = VirtualScrollState<IssueRowSort, ListContextParams>

  extension IssueVirtualizer {
    /// The issue list, as zbugs' list page sets it up: pages of
    /// `issueListV2` from a row's sort keys, and an issue looked up by its
    /// id or short id. Opened at `permalinkID`, or else where
    /// `scrollState` says.
    static func issues(
      _ zero: Zero<AuthData?>, filter: IssueFilter, permalinkID: String?,
      scrollState: IssueScrollState?
    ) -> IssueVirtualizer {
      IssueVirtualizer(
        zero: zero, listContext: filter.listContext, estimatedRowHeight: 56, rowKey: \.id,
        toStartRow: { IssueRowSort(id: $0.id, created: $0.created, modified: $0.modified) },
        pageQuery: { page, listContext in
          Queries.issueListV2(
            IssueListV2Args(
              listContext: listContext, limit: page.limit, start: page.start,
              dir: page.direction == .forward ? .forward : .backward,
              inclusive: page.start == nil))
        },
        rowQuery: { row, listContext in
          let (field, value) = IssueKey.parse(row.id)
          return Queries.listIssueByID(
            ListIssueByIDArgs(listContext: listContext, idField: field, idValue: value))
        }, permalinkID: permalinkID, scrollState: scrollState)
    }
  }

  /// The issue list in SwiftUI.
  struct IssueList: View {
    let list: IssueVirtualizer
    /// The issue the list highlights. A tap or a key sets it, even to the
    /// highlighted one.
    @Binding var selection: String?

    var body: some View {
      VirtualList(list, selection: $selection) { item, isSelected in
        IssueRow(issue: item.row, isSelected: isSelected)
      }
      .overlay {
        let s = list.snapshot
        if s.rowsEmpty {
          if s.complete {
            ContentUnavailableView("No Issues", systemImage: "checkmark.circle")
          } else {
            ProgressView()
          }
        }
      }
    }
  }

  struct IssueRow: View {
    let issue: Issue
    var isSelected = false

    var body: some View {
      VStack(alignment: .leading, spacing: 4) {
        HStack(alignment: .firstTextBaseline) {
          Image(systemName: issue.open ? "circle" : "checkmark.circle.fill")
            .foregroundStyle(issue.open ? Color.green : Color.purple)
          Text(issue.title).font(.headline).lineLimit(2)
        }
        Group {
          if (issue.labels ?? []).isEmpty {
            subtitle
          } else {
            // The labels after the subtitle, or below it in a narrow column.
            ViewThatFits(in: .horizontal) {
              HStack(spacing: 6) {
                subtitle
                labels
              }
              VStack(alignment: .leading, spacing: 4) {
                subtitle
                HStack(spacing: 6) { labels }
              }
            }
          }
        }
        .font(.caption)
        .foregroundStyle(.secondary)
      }
      .frame(maxWidth: .infinity, alignment: .leading)
      .padding(.horizontal, 16)
      .padding(.vertical, 8)
      #if os(iOS)
        // A plain list's separator.
        .overlay(alignment: .bottom) {
          if !isSelected { Divider().padding(.leading, 16) }
        }
      #endif
      .contentShape(.accessibility, Rectangle())
      .accessibilityElement(children: .combine)
      .accessibilityIdentifier("IssueRow")
    }

    private var subtitle: some View {
      Text(issue.subtitle).lineLimit(1)
    }

    /// Whole, never squeezed.
    private var labels: some View {
      ForEach(issue.labels ?? [], id: \.id) { label in
        Text(label.name)
          .padding(.horizontal, 6)
          .background(.quaternary, in: Capsule())
          .fixedSize()
      }
    }
  }

  extension Issue {
    /// `#123 · alice · 3 days ago`.
    var subtitle: String {
      var parts: [String] = []
      if let shortID { parts.append("#\(shortID)") }
      if let creator { parts.append(creator.login) }
      parts.append(
        Date(timeIntervalSince1970: modified / 1000).formatted(.relative(presentation: .named)))
      return parts.joined(separator: " · ")
    }
  }
#endif
