#!/usr/bin/env bash
set -euo pipefail

# Do toolchain setup outside the mounted repo/workspace.
cd "$HOME"

sudo corepack enable pnpm

# Configure pnpm to use the correct store directory
pnpm config set store-dir "$HOME/.local/share/pnpm/store"

cd /workspaces/mono

# Host macOS 1Password helper does not exist inside Linux.
# Use Linux ssh-keygen, which signs through the forwarded SSH_AUTH_SOCK agent.
git config --local gpg.ssh.program "$(command -v ssh-keygen)"
git config --local gpg.format ssh
git config --local commit.gpgsign true
git config --local tag.gpgsign true

# Remove npm and npx symlinks to force pnpm usage
sudo rm -rf \
  /usr/local/bin/npm \
  /usr/local/bin/npx \
  /usr/local/lib/node_modules/npm
