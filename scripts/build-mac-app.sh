#!/bin/zsh
# Builds GazaBridge.app (and optionally a DMG) on macOS.
#
# Release builds compile ~800 crates with link-time optimisation, which
# keeps every core busy for a few minutes. This script runs the build at low
# priority and on fewer cores (JOBS, default: half the cores) so the Mac
# stays usable and cooler; it takes a little longer.
#
#   scripts/build-mac-app.sh          # GazaBridge.app
#   scripts/build-mac-app.sh dmg      # also a .dmg
#   JOBS=8 scripts/build-mac-app.sh   # faster, hotter
set -euo pipefail

cd "$(dirname "$0")/../apps/desktop/src-tauri"

# Homebrew's Rust has no wasm target; prefer rustup's toolchain when present.
if [[ -d /opt/homebrew/opt/rustup/bin ]]; then
  export PATH="/opt/homebrew/opt/rustup/bin:$PATH"
fi
command -v trunk >/dev/null || { echo "trunk is missing: brew install trunk" >&2; exit 1; }
command -v npx >/dev/null || { echo "Node.js (npx) is missing" >&2; exit 1; }

JOBS="${JOBS:-$(( $(sysctl -n hw.ncpu) / 2 ))}"
BUNDLES="app"
[[ "${1:-}" == "dmg" ]] && BUNDLES="app,dmg"

nice -n 10 npx --yes @tauri-apps/cli@2.9 build --bundles "$BUNDLES" -- -j "$JOBS"

echo
echo "Built: $(cd ../../.. && pwd)/target/release/bundle/macos/GazaBridge.app"
