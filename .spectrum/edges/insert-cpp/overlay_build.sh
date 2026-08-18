#!/bin/sh
# Embedded build for the insert-cpp conformance edge. The extension's build
# closure (the DuckDB tree and the vcpkg dependencies) is too large to vendor, so
# {code} holds only the node's own src/ and this script overlays its files onto
# the live checkout, rebuilds the unittest binary (which statically links the
# extension), and restores the originals. $DUT_CHECKOUT names the checkout; the
# edge README states it. The build compiles one variant into unittest; the
# scenario runs execute that binary before the next variant overwrites it.
set -e
CODE="$1"
: "${DUT_CHECKOUT:?set DUT_CHECKOUT to the dut-spectrum checkout}"
SRC="$DUT_CHECKOUT/src"

# This overlay mutates the live checkout in place, so two builds running against
# the same $DUT_CHECKOUT at once would interleave and corrupt each other (the
# runner isolates its own scratch directory, not this external tree). Serialize
# with an atomic mkdir lock, held only for the overlay-build-restore span. mkdir
# is used rather than flock, which macOS does not ship. The lock is released by
# restore below, on both normal and errored exit; a killed build (SIGKILL) leaves
# it and a half-patched tree, which the message tells the next run how to clear.
LOCK="$DUT_CHECKOUT/.spectrum-overlay.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
  echo "overlay_build: $LOCK exists." >&2
  echo "  Another overlay build is running against $DUT_CHECKOUT, or a killed one" >&2
  echo "  left it behind. If no build is running, the checkout may be half-patched:" >&2
  echo "  restore it (git -C \"$DUT_CHECKOUT\" checkout -- src) and remove $LOCK." >&2
  exit 1
fi

BAK="$(mktemp -d)"
NEWLIST="$BAK/new.list"
: > "$NEWLIST"
mkdir -p "$BAK/orig"

restore() {
  if [ -d "$BAK/orig" ]; then
    ( cd "$BAK/orig" && find . -type f | while read -r f; do cp "$f" "$SRC/$f"; done )
  fi
  while read -r nf; do [ -n "$nf" ] && rm -f "$SRC/$nf"; done < "$NEWLIST"
  rm -rf "$BAK"
  rmdir "$LOCK" 2>/dev/null || true
}
trap restore EXIT

# Overlay only files that differ or are new, so ninja recompiles just those.
( cd "$CODE/src" && find . -type f ) | while read -r rel; do
  rel="${rel#./}"
  if [ -f "$SRC/$rel" ]; then
    if ! cmp -s "$CODE/src/$rel" "$SRC/$rel"; then
      mkdir -p "$BAK/orig/$(dirname "$rel")"
      cp "$SRC/$rel" "$BAK/orig/$rel"
      cp "$CODE/src/$rel" "$SRC/$rel"
    fi
  else
    mkdir -p "$(dirname "$SRC/$rel")"
    cp "$CODE/src/$rel" "$SRC/$rel"
    echo "$rel" >> "$NEWLIST"
  fi
done

ninja -C "$DUT_CHECKOUT/build/release" unittest 1>&2
