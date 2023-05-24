#!/bin/bash

[[ -z "$1" ]] && echo "first argument must be a key" && exit 1
[[ -z "$2" ]] && echo "second argument must be a secret" && exit 1
[[ -n "$3" && "$3" != "staging" ]] && echo "third argument must be empty or 'staging'" && exit 1

ENV="prod"
[[ "$3" == "staging" ]] && ENV="staging"

F=$(mktemp)
function cleanup()
{
  rm $F
}

trap cleanup EXIT

echo "{\"$1\": \"$2\"}" > $F

npm run wrangler-$ENV -- secret:bulk $F
