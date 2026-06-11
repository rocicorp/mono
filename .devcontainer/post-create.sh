#!/usr/bin/env bash
set -euo pipefail

# Do toolchain setup outside the mounted repo/workspace.
cd "$HOME"

sudo corepack enable pnpm

# Configure pnpm to use the correct store directory
pnpm config set store-dir "$HOME/.local/share/pnpm/store"

# Remove npm and npx symlinks to force pnpm usage
sudo rm -rf \
  /usr/local/bin/npm \
  /usr/local/bin/npx \
  /usr/local/lib/node_modules/npm
