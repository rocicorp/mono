#!/bin/bash

[[ -z "$1" ]] && echo "first argument must be a secret" && exit 1

NODE_PROGRAM="(async () => {\
const s='$1';\
const en=[...s].reduce((ui, _, i) => {\
  ui[i] = s[i].charCodeAt(0);\
  return ui\
}, new Uint8Array(s.length));\
const o=await crypto.subtle.digest('SHA-256', en);\
console.log(new Uint8Array(o).toString())\
})()"
SECRET=$(node -e "$NODE_PROGRAM")

echo "writing hashed value $SECRET ($1)"

./write-secret.sh NEW_ROOM_SECRET $1 $2
