# Rocicorp Monorepo

## SQLite build

Zero uses a non-trunk build of SQLite. To update the build:

1. Determine desired Check-in of SQLite.
   a. For example, https://sqlite.org/src/info/2a07caad4ab1bf5f is referred to as Check-in `2a07caad`.
   b. Verify that the `ZIP archive` link on the info page is of the form
   https://sqlite.org/src/zip/{CHECKIN}/SQLite-{CHECKIN}.zip
   c. For the time being, we are using the latest build referenced by bedrockdb, e.g.:
   https://github.com/Expensify/Bedrock/blob/a70564a677d4643c0761cea4eb528237474b006d/libstuff/sqlite3.h#L151

2. Edit the `CHECKIN=` variable in [deps/download.sh](deps/download.sh).

3. From the repo root, run:

```sh
npm run download-deps
npm install
```
