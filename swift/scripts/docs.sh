#!/bin/sh
# Builds the DocC catalogs, failing on any warning (a broken symbol link, an
# undocumented parameter). ZeroUI's symbols exist only on Apple platforms, so
# Linux builds the other two.
#
#   scripts/docs.sh [<module>...]     default: Zero ZeroUI ZeroTesting
#
# Writes .build/docs/<module>.doccarchive. The archives are for checking and
# for Xcode; they aren't transformed for static hosting, which fails in the
# Linux toolchain's docc.
set -eu
cd "$(dirname "$0")/.."

modules=${*:-Zero ZeroUI ZeroTesting}
graphs=$(swift package dump-symbol-graph --minimum-access-level public |
  sed -n 's/^Files written to //p')
docc=$(command -v docc || xcrun --find docc)

for m in $modules; do
  dir=.build/docs/symbolgraph-$m
  rm -rf "$dir" ".build/docs/$m.doccarchive"
  mkdir -p "$dir"
  # The module's own symbols, and its extensions of other modules' types.
  cp "$graphs/$m.symbols.json" "$dir/"
  for extension in "$graphs/$m@"*.symbols.json; do
    [ -e "$extension" ] && cp "$extension" "$dir/"
  done
  "$docc" convert "Sources/$m/$m.docc" \
    --additional-symbol-graph-dir "$dir" \
    --fallback-display-name "$m" \
    --fallback-bundle-identifier "dev.rocicorp.$m" \
    --output-path ".build/docs/$m.doccarchive" \
    --no-transform-for-static-hosting \
    --warnings-as-errors
done
