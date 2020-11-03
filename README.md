![Go](https://github.com/rocicorp/repc/workflows/Rust/badge.svg)

# repc

This is the core [Replicache](https://github.com/rocicorp/replicache) client.

It gets compiled to wasm and embedded by [replicache-sdk-js](https://github.com/rocicorp/replicache-sdk-js). In the future it will also be used in iOS, Android, and other SDKs.

## Release

```
go run ./tool/bump <newver>
git push origin
<land pr>
<fetch master>
git tag v<newver>
./tool/release.sh
git push origin v<newver>
# Github automatically shows the new tag at https://github.com/rocicorp/repc/releases
# Edit that release, add a description, and upload the objects
```

## FAQ

### Why is a project called "repc" written in Rust?

Yeah. It stands for "Replicache Client". Eventually, this project will probably just be renamed to "replicache" and take over the root repository.
