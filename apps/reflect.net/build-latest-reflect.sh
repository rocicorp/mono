#!/bin/bash

VENDOR="$(pwd)/vendor"
REFLECT_ONLY=false
[[ $1 == "reflect" ]] && REFLECT_ONLY=true
SERVER_ONLY=false
[[ $1 == "reflect-server" ]] && SERVER_ONLY=true
if [[ $REFLECT_ONLY != true ]] && [[ $SERVER_ONLY != true ]]; then
  rm ./vendor/rocicorp-reflect-*
fi
cd ../mono
[[ $1 == "pull" ]] && git stash -u && git pull && git stash pop
npm install
npm run build
if [[ $SERVER_ONLY != true ]]; then
  echo "Building and packaging reflect"
  npm pack --workspace ./packages/reflect --pack-destination $VENDOR
  REFLECT_V=$(npm pkg get version --prefix ./packages/reflect | sed 's/"//g')
fi
if [[ $REFLECT_ONLY != true ]]; then
  echo "Building and packaging reflect-server"
  npm pack --workspace ./packages/reflect-server --pack-destination $VENDOR
  REFLECT_SERVER_V=$(npm pkg get version --prefix ./packages/reflect-server | sed 's/"//g')
fi
BUILT_SHA=$(git rev-parse HEAD)
cd -

echo "updating packages"
[[ $SERVER_ONLY != true ]] && echo "@rocicorp/reflect $REFLECT_V"
[[ $REFLECT_ONLY != true ]] && echo "@rocicorp/reflect-server $REFLECT_SERVER_V"

REFLECT_TARBALL_PATH="./vendor/rocicorp-reflect-$REFLECT_V-$BUILT_SHA.tgz"
REFLECT_SERVER_TARBALL_PATH="./vendor/rocicorp-reflect-server-$REFLECT_SERVER_V-$BUILT_SHA.tgz"
[[ $SERVER_ONLY != true ]] && mv "./vendor/rocicorp-reflect-$REFLECT_V.tgz" $REFLECT_TARBALL_PATH
[[ $REFLECT_ONLY != true ]] && mv "./vendor/rocicorp-reflect-server-$REFLECT_SERVER_V.tgz" $REFLECT_SERVER_TARBALL_PATH

[[ $SERVER_ONLY != true ]] && npm add $REFLECT_TARBALL_PATH
[[ $REFLECT_ONLY != true ]] && npm add $REFLECT_SERVER_TARBALL_PATH
