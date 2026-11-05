#!/bin/sh
# Regenerate the pre-generated bindings used by the `bundled` feature.
#
# Usage: ./regenerate-bindings.sh [TARGET]
#
# Without TARGET, bindings are generated for the host (any 64-bit Unix).
# Use TARGET=x86_64-pc-windows-gnu for the Windows bindings.
set -eu

cd "$(dirname "$0")"

target="${1:-}"
case "$target" in
    *-windows-gnu) output="src/bindgen_bundled_windows_gnu.rs" ;;
    *) output="src/bindgen_bundled_unix.rs" ;;
esac

target_dir="${CARGO_TARGET_DIR:-../target}/regenerate-bindings"
rm -rf "$target_dir"
cargo build --features bundled,buildtime_bindgen --target-dir "$target_dir" ${target:+--target "$target"}

cp "$(find "$target_dir" -path '*/libmdb-sys-*/out/bindings.rs')" "$output"
echo "Wrote $output"
