NOTE: This file is HACKING.md, not README.md, because npmjs.com shows the
README automatically on the package homepage.

# Cutting a Canary Release

- Sketch the release notes by copying an earlier version and reviewing history from last tag
- If necessary choose a new major/minor version and commit it to main. The canary script below only increments the patch version.
- Run `node ./packages/reflect/tool/create-canary.js`

# Test a Release

## Docs

Follow the instructions at https://hello.reflect.net/scaffold and https://hello.reflect.net/add-to-existing except use @$NEW_VERSION instead of @latest.

## Examples

Checkout each of the examples at https://hello.reflect.net/start/examples.

Install the new version from npm:

```
npm install @rocicorp/reflect@canary
```

Follow instructions in repo to finish setting up app, and run to make sure it works.

For canary releases you may need to make changes to the app to allow it work, or to adopt newer features. Publish these to a WIP PR which we can update until release.

# Release an Official Build

TODO, but this will be something like checking out the good canary, changing the version, publishing again, then updating all the apps.

## Release a Server to more channels

```bash
cd mirror/mirror-cli
npm run mirror releaseServer -- --server=0.40.5 --channels=stable
```

The command is additive and will not remove any existing channels (e.g. "canary").
This can also be used for one-off debugging, e.g. if you want to release to a specific
app. Define your temporary channel name (e.g. "debug-#433") and release the desired server to
that channel. Then in the [Firebase console](https://console.firebase.google.com/project/reflect-mirror-prod/firestore/data/~2Fapps), navigate to the `apps/${appID}` document and set that App's
`serverReleaseChannel` to your channel name. The deployment should automatically kick in.
Don't forget to return the App to its original channel (and clean up the server doc).

## Unrelease a Server from a channel

```bash
cd mirror/mirror-cli
npm run mirror unreleaseServer -- --server=0.40.5 --channels=stable
```

Essentially, `releaseServer` and `unreleaseServer` take the same cli arguments.

## Write Release Notes

Add Release Notes to [Notion](https://www.notion.so/replicache/Release-Notes-43b93bd9bf774de6a505247a6e7a3fb8) following the pattern of
previous release notes.

Publish the page and share it to the [#reflect-alpha Discord channel](https://discord.gg/9PzrG5Qv).

---
