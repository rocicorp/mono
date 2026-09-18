#!/usr/bin/env bash
# sign-branch — take over a branch's commits as their author, signed with YOUR key.
#
#   scripts/sign-branch.sh [<branch>] [--yes] [--no-push]
#
# With no branch, the ten most recently pushed branches on origin are listed and one is
# picked with the arrow keys (or j/k, a digit, Enter; q to quit).
#
# The rocicorp org requires every commit to be SSH-signed by a key in rocicorp/.github's
# `signing/allowed_signers`; in this repo the "Verify signed commit authors" job of
# `.github/workflows/dev-release.yml` enforces it. An agent-pushed commit carries a good
# signature made by a key that is not an allowed principal, so that check rejects it.
# This rebuilds those commits as yours, signed by your key, keeping the original author
# as a `Co-authored-by:` trailer and preserving the author date.
#
# Ported from rocicorp/rindle (scripts/sign-branch.sh).
#
# Nothing is checked out. A signature is a field of the commit object, not a property of
# the working tree, so each commit is rebuilt with `git commit-tree` from the tree the
# original already points at — your working tree, index and current branch are never
# touched, and it runs fine with uncommitted changes from any branch. Reusing trees by
# OID means the content is identical by construction and can never conflict. Works in a
# bare or blobless clone.
#
# Only the named branch's OWN commits are touched: the range is everything it added since
# it forked from origin's default branch, so shared history can never be rewritten. That
# fork point is derived (`ls-remote --symref` for the default branch, then `merge-base`),
# never asked for.
#
# Within that range it starts at the FIRST commit that needs rebuilding — one whose author
# is not you, or that carries no signature — and runs to the tip, so commits already
# authored and signed by you keep their SHA and their completed checks. Merges are refused
# rather than flattened. It force-pushes, with the lease pinned to the SHA it fetched, so
# it asks first unless you pass --yes.
set -euo pipefail

self=$(cd "$(dirname "$0")" && pwd)/$(basename "$0")
die() { echo "sign-branch: $*" >&2; exit 1; }
usage() { awk 'NR > 1 && !/^#/ { exit } NR > 1 { sub(/^# ?/, ""); print }' "$self"; }

branch=; assume_yes=0; push=1
while [ $# -gt 0 ]; do
  case "$1" in
    --yes|-y)  assume_yes=1; shift ;;
    --no-push) push=0; shift ;;
    -h|--help) usage; exit 0 ;;
    -*)        die "unknown flag: $1" ;;
    *)         [ -z "$branch" ] || die "only one branch, got '$branch' and '$1'"
               branch=$1; shift ;;
  esac
done
git rev-parse --git-dir >/dev/null 2>&1 || die "not inside a git repository"

# Fail before building anything if signing cannot produce what the org accepts.
me_name=$(git config user.name)   || die "no user.name configured"
me_email=$(git config user.email) || die "no user.email configured"
git config --get user.signingkey >/dev/null \
  || die "no user.signingkey configured — set it to your SSH signing key first"
[ "$(git config --default openpgp --get gpg.format)" = ssh ] \
  || die "gpg.format is not 'ssh' — allowed_signers only accepts SSH signatures:
    git config --global gpg.format ssh"

# Ask the remote what its default branch is, rather than reading a local ref: this works
# in a bare or fresh clone that has no refs/remotes/* at all, and cannot go stale.
default=$(git ls-remote --symref origin HEAD | sed -n 's|^ref: refs/heads/||p' | awk '{print $1}')
[ -n "$default" ] || die "cannot determine origin's default branch"

# Arrow-key menu on the terminal: sets $picked to the chosen index. Up/Down or k/j move,
# a digit jumps, Enter picks, q or Esc aborts. Redraws in place with plain escapes so it
# needs nothing beyond bash 3.2 and a VT100-ish terminal.
menu() {
  local n=$# cur=0 i key
  local items=("$@")
  draw() {
    for ((i = 0; i < n; i++)); do
      if [ "$i" = "$cur" ]; then printf '\033[K \033[7m> %d) %s\033[0m\n' $((i + 1)) "${items[i]}"
      else printf '\033[K   %d) %s\n' $((i + 1)) "${items[i]}"; fi
    done
  }
  tput civis 2>/dev/null || true
  trap 'tput cnorm 2>/dev/null || true' EXIT
  draw
  while :; do
    IFS= read -rsn1 key
    # An arrow arrives as ESC [ A; a bare ESC has nothing following it.
    if [ "$key" = $'\033' ]; then IFS= read -rsn2 -t 1 key || key=esc; fi
    case "$key" in
      '[A' | k) cur=$(((cur + n - 1) % n)) ;;
      '[B' | j) cur=$(((cur + 1) % n)) ;;
      [1-9])    [ "$key" -le "$n" ] && { cur=$((key - 1)); break; } ;;
      '')       break ;;   # Enter: -n1 hands back an empty key at the delimiter
      q | esc)  printf '\n'; die "aborted" ;;
    esac
    printf '\033[%dA' "$n"; draw
  done
  picked=$cur
}

# No branch named: offer the most recently pushed branches on origin. Fetched into
# refs/remotes/origin/* explicitly so this also works in a bare clone, which has none.
recent=10
if [ -z "$branch" ]; then
  [ -t 0 ] && [ -t 1 ] || die "no branch given, and no terminal to pick one from"
  echo "fetching branches from origin…"
  git fetch --quiet --prune origin '+refs/heads/*:refs/remotes/origin/*' \
    || die "cannot fetch branches from origin"
  names=(); labels=()
  while IFS=$'\t' read -r name date author subject; do
    case "$name" in "$default" | HEAD) continue ;; esac
    names+=("$name")
    labels+=("$(printf '%-36.36s  %-14.14s  %-16.16s  %.40s' "$name" "$date" "$author" "$subject")")
    [ "${#names[@]}" -lt "$recent" ] || break
  done < <(git for-each-ref --sort=-committerdate \
             --format='%(refname:lstrip=3)%09%(committerdate:relative)%09%(authorname)%09%(contents:subject)' \
             refs/remotes/origin/)
  [ "${#names[@]}" -gt 0 ] || die "origin has no branches other than $default"
  echo
  echo "which branch to sign?  (${#names[@]} most recently pushed; tip author shown)"
  menu "${labels[@]}"
  branch=${names[$picked]}
  echo
fi

[ "$branch" != "$default" ] || die "$branch IS origin's default branch — refusing to rewrite it"

echo "fetching origin…"
# FETCH_HEAD, not origin/<branch>: a bare clone has no remote-tracking refs, and it is
# precisely the SHA just fetched — which is what the push lease below must pin. Fetch the
# default branch first, since each fetch overwrites FETCH_HEAD.
git fetch --quiet origin "$default" || die "cannot fetch origin/$default"
default_sha=$(git rev-parse FETCH_HEAD^{commit})
git fetch --quiet origin "$branch" || die "no branch '$branch' on origin"
head_sha=$(git rev-parse FETCH_HEAD^{commit})

# The fork point, so only what this branch added is ever in scope.
base=$(git merge-base "$default_sha" "$head_sha") || die "$branch shares no history with $default"
[ "$(git rev-list --count "$base..$head_sha")" -gt 0 ] \
  || die "$branch adds nothing on top of $default — nothing to do"

# The raw object is the signature check: `git log --format=%G?` only reports a *verified*
# signature, which needs gpg.ssh.allowedSignersFile set, and its absence would otherwise
# look identical to an unsigned commit.
needs_fix() {
  [ "$(git log -1 --format='%ae' "$1")" = "$me_email" ] || return 0
  git cat-file commit "$1" | sed -n '/^$/q;p' | grep -q '^gpgsig ' || return 0
  return 1
}

first_bad=
while read -r sha; do
  if needs_fix "$sha"; then first_bad=$sha; break; fi
done < <(git rev-list --reverse "$base..$head_sha")
[ -n "$first_bad" ] \
  || { echo "every commit on $branch is already authored and signed by you"; exit 0; }

upstream=$(git rev-parse "$first_bad^")
echo
echo "branch: $branch    forked from: $default    author: $me_name <$me_email>"
if [ "$(git rev-list --count "$base..$upstream")" -gt 0 ]; then
  echo "keeping untouched:"
  git log --reverse --format='    %h  %an  %s' "$base..$upstream"
fi
echo "rebuilding:"
git log --reverse --format='    %h  %an  %s' "$upstream..$head_sha"
echo

if [ "$assume_yes" != 1 ]; then
  printf 'rebuild and force-push to origin/%s? [y/N] ' "$branch"
  read -r reply
  case "$reply" in y | Y | yes | YES) ;; *) die "aborted" ;; esac
fi

# Rebuilding passes one -p, which would silently flatten a merge into its first side.
# Checked up front so nothing is built before refusing.
merge=$(git rev-list --merges "$upstream..$head_sha" | head -1)
[ -z "$merge" ] \
  || die "$(git rev-parse --short "$merge") is a merge — rewrite this branch by hand"

new=$upstream
while read -r sha; do
  orig_email=$(git log -1 --format='%ae' "$sha")
  trailer=()
  if [ "$orig_email" != "$me_email" ]; then
    trailer=(--trailer "Co-authored-by: $(git log -1 --format='%an' "$sha") <$orig_email>")
  fi
  # ${arr[@]+...} so an empty array is not an unbound-variable error under bash 3.2,
  # which is still what macOS ships as /bin/bash.
  msg=$(git log -1 --format='%B' "$sha" \
        | git interpret-trailers --if-exists doNothing ${trailer[@]+"${trailer[@]}"})
  new=$(
    export GIT_AUTHOR_NAME=$me_name GIT_AUTHOR_EMAIL=$me_email
    export GIT_AUTHOR_DATE=$(git log -1 --format='%aI' "$sha")
    export GIT_COMMITTER_NAME=$me_name GIT_COMMITTER_EMAIL=$me_email
    printf '%s\n' "$msg" | git commit-tree "$sha^{tree}" -p "$new" -S -F -
  )
  echo "    $(git rev-parse --short "$sha") -> $(git rev-parse --short "$new")  $(git log -1 --format=%s "$sha")"
done < <(git rev-list --reverse "$upstream..$head_sha")

# Park the chain on a ref so it survives gc, and so --no-push leaves something to look at.
staging=refs/sign-branch/$branch
git update-ref "$staging" "$new"

if [ "$push" != 1 ]; then
  echo "not pushed (--no-push) — rebuilt chain at $staging ($(git rev-parse --short "$new"))"
  echo "  inspect: git log $base..$staging"
  echo "  push:    git push --force-with-lease=$branch:$head_sha origin $staging:refs/heads/$branch"
  exit 0
fi

git push --force-with-lease="$branch:$head_sha" origin "$new:refs/heads/$branch"
git update-ref -d "$staging"
echo "pushed — your local $branch is now behind; git fetch when you next need it"
