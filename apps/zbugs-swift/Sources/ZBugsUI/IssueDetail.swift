#if canImport(SwiftUI)
  import Foundation
  import SwiftUI
  import ZBugs
  import Zero
  import ZeroUI
  import ZeroVirtual

  /// An issue's comments, paged in as they scroll.
  typealias CommentVirtualizer = ZeroVirtualizer<
    AuthData?, Comment, String, CommentsPageArgs.Cursor, String
  >

  extension CommentVirtualizer {
    /// The issue's comments, oldest first, as zbugs' issue page sets them
    /// up: pages of `commentsPage` from a comment's time, its list context
    /// the issue's id.
    static func comments(_ zero: Zero<AuthData?>, issueID: String) -> CommentVirtualizer {
      CommentVirtualizer(
        zero: zero, listContext: issueID, estimatedRowHeight: 200, rowKey: \.id,
        toStartRow: { CommentsPageArgs.Cursor(id: $0.id, created: $0.created) },
        pageQuery: { page, issueID in
          Queries.commentsPage(
            CommentsPageArgs(
              issueID: issueID, limit: page.limit, start: page.start,
              dir: page.direction == .forward ? .forward : .backward,
              inclusive: page.start == nil))
        },
        rowQuery: { row, _ in Queries.comment(row.id) },
        // Comments are tall: about three screens of them.
        minPageSize: 20)
    }
  }

  /// One issue with its comments, as zbugs' issue page shows it.
  struct IssueDetail: View {
    @ZeroQuery<Issue, Issue?, AuthData?> var issue: Issue?
    @State private var comments: CommentVirtualizer
    @Environment(AppModel.self) private var model

    init(id: String, zero: Zero<AuthData?>) {
      _issue = ZeroQuery(Queries.issueDetail(IssueDetailArgs(idField: .id, id: .string(id))))
      _comments = State(initialValue: .comments(zero, issueID: id))
    }

    var body: some View {
      if let issue {
        IssueContent(issue: issue, comments: comments)
      } else if $issue.resultType == .complete {
        ContentUnavailableView("Issue Not Found", systemImage: "questionmark.circle")
      } else {
        ProgressView()
      }
    }
  }

  private struct IssueContent: View {
    let issue: Issue
    let comments: CommentVirtualizer
    @Environment(AppModel.self) private var model
    @State private var comment = ""
    @State private var editing = false

    var body: some View {
      // The header and the description above the comments, the composer
      // below them, in one list.
      VirtualList(comments) { item in
        CommentView(comment: item.row).column().padding(.bottom, 16)
      } header: {
        VStack(alignment: .leading, spacing: 16) {
          header
          if !issue.description.isEmpty {
            Text(markdown(issue.description)).textSelection(.enabled)
          }
          Divider()
        }
        .column()
        .padding(.top)
        .padding(.bottom, 16)
      } footer: {
        composer.column().padding(.bottom)
      }
      // String(_:), or the title formats the number for the locale (3,000).
      .navigationTitle(issue.shortID.map { "Issue #" + String($0) } ?? "Issue")
      .toolbar {
        if canEdit {
          ToolbarItem {
            Button(
              issue.open ? "Close Issue" : "Reopen",
              systemImage: issue.open
                ? "checkmark.circle" : "arrow.uturn.backward.circle"
            ) {
              model.mutate(
                Mutators.updateIssue(
                  UpdateIssueArgs(id: issue.id, open: !issue.open, modified: now())))
            }
          }
          ToolbarItem {
            Button("Edit", systemImage: "pencil") { editing = true }
          }
        }
      }
      .sheet(isPresented: $editing) {
        IssueEditor(issue: issue)
      }
    }

    private var header: some View {
      VStack(alignment: .leading, spacing: 8) {
        Text(issue.title).font(.title2.bold()).textSelection(.enabled)
        HStack(spacing: 8) {
          SwiftUI.Label(
            issue.open ? "Open" : "Closed",
            systemImage: issue.open ? "circle" : "checkmark.circle.fill"
          )
          .foregroundStyle(issue.open ? Color.green : Color.purple)
          if let creator = issue.creator { Text("by \(creator.login)") }
          if let assignee = issue.assignee { Text("· assigned to \(assignee.login)") }
          if issue.visibility == .internal {
            SwiftUI.Label("Internal", systemImage: "lock")
          }
        }
        .font(.subheadline)
        .foregroundStyle(.secondary)
        if let labels = issue.labels, !labels.isEmpty {
          HStack {
            ForEach(labels, id: \.id) { label in
              Text(label.name)
                .font(.caption)
                .padding(.horizontal, 8)
                .padding(.vertical, 2)
                .background(.quaternary, in: Capsule())
            }
          }
        }
      }
    }

    private var composer: some View {
      VStack(alignment: .trailing) {
        TextField("Add a comment", text: $comment, axis: .vertical)
          .lineLimit(3...8)
          .textFieldStyle(.roundedBorder)
        Button("Comment") {
          model.mutate(
            Mutators.addComment(
              AddCommentArgs(id: makeID(), issueID: issue.id, body: comment, created: now())))
          comment = ""
        }
        .disabled(model.auth == nil || comment.trimmingCharacters(in: .whitespaces).isEmpty)
      }
    }

    private var canEdit: Bool {
      guard let auth = model.auth else { return false }
      return auth.role == .crew || auth.sub == issue.creatorID
    }
  }

  private struct CommentView: View {
    let comment: Comment

    var body: some View {
      VStack(alignment: .leading, spacing: 4) {
        HStack {
          Text(comment.creator?.login ?? "…").bold()
          Text(
            Date(timeIntervalSince1970: comment.created / 1000)
              .formatted(.relative(presentation: .named))
          )
          .foregroundStyle(.secondary)
        }
        .font(.subheadline)
        Text(markdown(comment.body)).textSelection(.enabled)
      }
      .frame(maxWidth: .infinity, alignment: .leading)
      .padding(12)
      .background(.background.secondary, in: RoundedRectangle(cornerRadius: 8))
    }
  }

  extension View {
    /// In the issue page's column: at most 760 points wide, centered, with
    /// the standard margins.
    func column() -> some View {
      frame(maxWidth: 760, alignment: .leading)
        .padding(.horizontal)
        .frame(maxWidth: .infinity)
    }
  }

  /// Edits an issue's title and description.
  struct IssueEditor: View {
    let issue: Issue
    @Environment(AppModel.self) private var model
    @Environment(\.dismiss) private var dismiss
    @State private var title: String
    @State private var description: String

    init(issue: Issue) {
      self.issue = issue
      _title = State(initialValue: issue.title)
      _description = State(initialValue: issue.description)
    }

    var body: some View {
      NavigationStack {
        Form {
          TextField("Title", text: $title)
          TextField("Description", text: $description, axis: .vertical)
            .lineLimit(5...20)
        }
        .formStyle(.grouped)
        .navigationTitle("Edit Issue")
        .toolbar {
          ToolbarItem(placement: .cancellationAction) {
            Button("Cancel") { dismiss() }
          }
          ToolbarItem(placement: .confirmationAction) {
            Button("Save") {
              model.mutate(
                Mutators.updateIssue(
                  UpdateIssueArgs(
                    id: issue.id, title: title, description: description, modified: now())))
              dismiss()
            }
            .disabled(title.isEmpty)
          }
        }
      }
      // A sheet on iOS is the screen's width.
      #if os(macOS)
        .frame(minWidth: 420, minHeight: 300)
      #endif
    }
  }

  /// Markdown as `Text` shows it, or the plain text when it isn't.
  func markdown(_ s: String) -> AttributedString {
    (try? AttributedString(
      markdown: s, options: .init(interpretedSyntax: .inlineOnlyPreservingWhitespace)))
      ?? AttributedString(s)
  }

  /// Milliseconds since 1970, zbugs' timestamps.
  func now() -> Double {
    (Date().timeIntervalSince1970 * 1000).rounded()
  }
#endif
