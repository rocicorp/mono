![Go](https://github.com/rocicorp/repc/workflows/Rust/badge.svg)

# repc

This is a Rust port of [replicache-client](https://github.com/rocicorp/replicache-client).

## Initial Goals

The initial strategy is to move fast and get all the way through to a drop-in replacement to replicache-client as quickly as possible.

Things that don't matter for this initial pass:

- Optimizing, at all (as long as we think the general approach can be optimized)
- Clients other than https://github.com/rocicorp/replicache-sdk-js
- Environments other than wasm
- Idiomatic-ness (we are learning Rust at same time, so removing this from worry list will make us go waaaay faster)

Things that do matter:

- Continuous tests
- Basic Perf Benchmarks (just so we know where we are, espec relative to Go)
- Monitoring of WASM bundle size

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
