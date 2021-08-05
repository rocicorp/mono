![Go](https://github.com/rocicorp/repc/workflows/Rust/badge.svg)

# repc

This is the core [Replicache](https://github.com/rocicorp/replicache) client.

It gets compiled to wasm and embedded by [replicache](https://github.com/rocicorp/replicache). In the future it could also be used in iOS, Android, and other SDKs.

## Release

```
cd tool/bump
go run . repc <newver with no v> --root ../..
git push origin
<land pr>
<fetch master>
git tag v<newver>
cd ../..
# Our GH action builds and uploads repc.zip
git push origin v<newver>
# Github automatically shows the new tag at https://github.com/rocicorp/repc/releases
# Edit that release, add a description, and upload the objects
```

## FAQ

### Why is a project called "repc" written in Rust?

Yeah. It stands for "Replicache Client". Maybe it should have been rep-rs, or something. Oh well.
