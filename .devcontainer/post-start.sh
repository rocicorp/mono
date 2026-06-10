#!/usr/bin/env bash
set -euo pipefail

if ! git config --get user.name >/dev/null; then
  echo "WARN: git user.name is not configured."
fi

if ! git config --get user.email >/dev/null; then
  echo "WARN: git user.email is not configured."
fi

if ! git config --get user.signingkey >/dev/null; then
  echo "WARN: git user.signingkey is not configured."
fi

if ! ssh-add -L >/dev/null 2>&1; then
  echo "WARN: SSH agent has no visible keys. Commit signing through 1Password/SSH agent may not work."
fi
