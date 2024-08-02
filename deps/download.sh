#!/usr/bin/env bash

# ===
# This script defines and generates the bundled SQLite3 unit (sqlite3.c).
# Adapted from https://github.com/WiseLibs/better-sqlite3/blob/master/deps/download.sh
#
# The following steps are taken:
# 1. populate the shell environment with the defined compile-time options.
# 2. download and extract the SQLite3 source code into a temporary directory.
# 3. run "sh configure" and "make sqlite3.c" within the source directory.
# 4. copy the generated amalgamation into the output directory (./sqlite3).
# ===

CHECKIN="2a07caad"

# Defines below are sorted alphabetically
DEFINES="
HAVE_INT16_T=1
HAVE_INT32_T=1
HAVE_INT8_T=1
HAVE_STDINT_H=1
HAVE_UINT16_T=1
HAVE_UINT32_T=1
HAVE_UINT8_T=1
HAVE_USLEEP=1
SQLITE_DEFAULT_CACHE_SIZE=-16000
SQLITE_DEFAULT_FOREIGN_KEYS=1
SQLITE_DEFAULT_MEMSTATUS=0
SQLITE_DEFAULT_WAL_SYNCHRONOUS=1
SQLITE_DQS=0
SQLITE_ENABLE_COLUMN_METADATA
SQLITE_ENABLE_DESERIALIZE
SQLITE_ENABLE_FTS3
SQLITE_ENABLE_FTS3_PARENTHESIS
SQLITE_ENABLE_FTS4
SQLITE_ENABLE_FTS5
SQLITE_ENABLE_GEOPOLY
SQLITE_ENABLE_JSON1
SQLITE_ENABLE_MATH_FUNCTIONS
SQLITE_ENABLE_RTREE
SQLITE_ENABLE_STAT4
SQLITE_ENABLE_UPDATE_DELETE_LIMIT
SQLITE_LIKE_DOESNT_MATCH_BLOBS
SQLITE_OMIT_DEPRECATED
SQLITE_OMIT_PROGRESS_CALLBACK
SQLITE_OMIT_SHARED_CACHE
SQLITE_OMIT_TCL_VARIABLE
SQLITE_SOUNDEX
SQLITE_THREADSAFE=2
SQLITE_TRACE_SIZE_LIMIT=32
SQLITE_USE_URI=0
"

# ========== START SCRIPT ========== #

echo "setting up environment..."
DEPS="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
TEMP="$DEPS/temp"
OUTPUT="$DEPS/sqlite3"
rm -rf "$TEMP"
rm -rf "$OUTPUT"
mkdir -p "$TEMP"
mkdir -p "$OUTPUT"
export CFLAGS=`echo $(echo "$DEFINES" | sed -e "/^\s*$/d" -e "s/^/-D/")`

echo "downloading source..."
curl -#f "https://sqlite.org/src/zip/$CHECKIN/SQLite-$CHECKIN.zip" > "$TEMP/source.zip" || exit 1

echo "extracting source..."
unzip "$TEMP/source.zip" -d "$TEMP" > /dev/null || exit 1
cd "$TEMP/SQLite-$CHECKIN" || exit 1

echo "configuring amalgamation..."
sh configure > /dev/null || exit 1

echo "building amalgamation..."
make sqlite3.c > /dev/null || exit 1

echo "copying amalgamation..."
cp sqlite3.c sqlite3.h sqlite3ext.h "$OUTPUT/" || exit 1

echo "cleaning up..."
cd - > /dev/null || exit 1
rm -rf "$TEMP"

echo "done!"
