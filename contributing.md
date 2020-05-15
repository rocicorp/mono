## Replicache Contributing Guide

We welcome contributions, questions, and feedback from the community. Here's a short guide
to how to work together.

### Bug Reports & Discussions

* File all Replicache issues in the Replicache repo https://github.com/rocicorp/replicache/issues. 
   * This simplifies our view of what's in flight and doesn't require anyone to understand how our repos are organized.
* Join our [Slack channel](https://join.slack.com/t/rocicorp/shared_invite/zt-dcez2xsi-nAhW1Lt~32Y3~~y54pMV0g) for realtime help or discussion.

### Making changes

* We subscribe heavily to the practice of [talk, then code](https://dave.cheney.net/2019/02/18/talk-then-code).
   * Foundational, tricky, or wide-reaching changes should be discussed ahead of time on an issue in order to maximize the chances that they land well. Typically this involves a discussion of requirements and constraints and a design sketch showing major interfaces and logic. ([example](https://github.com/rocicorp/replicache/issues/27), [example](https://github.com/rocicorp/replicache/issues/30))
* We sometimes use [tags with these meanings](https://news.ycombinator.com/item?id=23027988) in code reviews
   
### Style

* If possible, do not intentionally crash the process (eg, by `panic`ing on unexpected conditions).
   * There are of course times where `panic`ing is appropriate, where execution simply cannot continue. But there are also times when the process might be able to continue doing other useful work unrelated to the problem, so be judicious with `panic`s.
* Prefer a consumer-define interface to introduce seams for testing (as opposed to say variables that point to an implementation, functions that take functions, etc)
* Use log levels consistently:
   * **ERROR**: something truly unexpected has happened **and a developer should go look**.
      * Examples that might be ERRORs:
         * an important invariant has been violated
         * stored data is corrupt
      * Examples that probably are *not* ERRORs:
         * an http request failed
         * couldn't parse user input
         * a thing that can time out timed out, or a thing that can fail failed
   * **INFO**: information that is immediately useful to the developer, or an important change of state. Info logs should not be spammy.
      * Examples that might be INFOs:
         * "Server listening on port 1234..."
         * udpated foo to a new version
      * Examples that probably are *not* INFOs:
         * server received a request
         * successfully completed a periodic task that is not an important change of state (eg, logs were rotated)
   * **DEBUG**: verbose information about what's happening that might be useful to a developer.
      * Examples that probably are DEBUGs:
         * request and response content
         * some process is starting
* Code must be gofmt'd but does *not* have to lint
* Prefer intialization with a literal to `make` eg `foo := map[string]string{}` not `foo := make(map[string]string)`
   * TODO(aboodman) say why
