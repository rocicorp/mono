---
'zero-swift': minor
---

The CLI is built on [incur](https://github.com/wevm/incur). Errors print a code and a message and exit 1. `--json` prints results as JSON, `--llms` prints a manifest for coding agents, and `skills add` installs the commands as an agent skill. `codegen` has an option for every config key, such as `--type-prefix` and `--access`. The schema module option is now `--schema-file`, because incur reserves `--schema`; `--schema <path>` still works. The config keys are now documented in the README.
