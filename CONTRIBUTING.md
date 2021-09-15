## Replicache Contributing Guide

We welcome contributions, questions, and feedback from the community. Here's a short guide
to how to work together.

### Bug Reports & Discussions

- File all Replicache issues in the Replicache repo https://github.com/rocicorp/replicache/issues.
  - This simplifies our view of what's in flight and doesn't require anyone to understand how our repos are organized.
- Join our [Slack channel](https://join.slack.com/t/rocicorp/shared_invite/zt-dcez2xsi-nAhW1Lt~32Y3~~y54pMV0g) for realtime help or discussion.

### Making changes

- We subscribe heavily to the practice of [talk, then code](https://dave.cheney.net/2019/02/18/talk-then-code).
  - Foundational, tricky, or wide-reaching changes should be discussed ahead of time on an issue in order to maximize the chances that they land well. Typically this involves a discussion of requirements and constraints and a design sketch showing major interfaces and logic. ([example](https://github.com/rocicorp/replicache/issues/27), [example](https://github.com/rocicorp/replicache/issues/30))
- Code review
  - Rocicorp partners prefer to get async after-merge code reviews.
    - Reviewer should review within 3 days.
    - Reviewee should respond within 7 days.
  - We sometimes use [tags with these meanings](https://news.ycombinator.com/item?id=23027988) in code reviews

### Legal

All contributors must sign a contributor agreement, either for an <a href="https://rocicorp.github.io/ca/individual.html">individual</a> or <a href="https://rocicorp.github.io/ca/corporation.html">corporation</a>, before a pull request can be accepted.

### Style (General)

- If possible, do not intentionally crash processes (eg, by `panic`ing on unexpected conditions).
  - For our client-side software, this is bad practice because we frequently run in-process with our users and we will crash them too!
  - For our server-side software, this is bad practice because we will lose any other requests that were also in-flight on that server.
  - There are other types of software in which crashing early and often may be more appropriate, but for consistency and code reuse reasons we generally avoid crashing everywhere.
- We use three log levels. Please employ them in a way that's consistent with the following semantics:
  - **ERROR**: something truly unexpected has happened **and a developer should go look**.
    - Examples that might be ERRORs:
      - an important invariant has been violated
      - stored data is corrupt
    - Examples that probably are _not_ ERRORs:
      - an http request failed
      - couldn't parse user input
      - a thing that can time out timed out, or a thing that can fail failed
  - **INFO**: information that is immediately useful to the developer, or an important change of state. Info logs should not be spammy.
    - Examples that might be INFOs:
      - "Server listening on port 1234..."
      - udpated foo to a new version
    - Examples that probably are _not_ INFOs:
      - server received a request
      - successfully completed a periodic task that is not an important change of state (eg, logs were rotated)
  - **DEBUG**: verbose information about what's happening that might be useful to a developer.
    - Examples that probably are DEBUGs:
      - request and response content
      - some process is starting
  - We do not use a warning level because warnings are typically not actionable and are mostly ignored. We prefer the developer to take a position: does the thing rise to the level that a developer should go do something about it or not? We do not use a trace level because we haven't yet found a use for it, and extra log levels are just confusing.

### Style (Go-specific)

- Prefer consumer-defined interfaces to introduce seams for testing (as opposed to say variables that point to an implementation, functions that take functions, etc)
- Code must be gofmt'd but does _not_ have to lint
- There are a lot of ways to initialize variables in Go. For consistency, we default to literal-style initialization (e.g., `Foo{}` or `map[string]string{}`) because it's a few chars shorter. We use `make` or `new` when necessary, e.g., to create a slice with a specific capacity or to create a channel.
