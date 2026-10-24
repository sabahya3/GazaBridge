#!/usr/bin/env python3
"""Regenerates the desktop application icons from docs/branding/logo-mark.svg.

Requires `rsvg-convert` (librsvg). On macOS `iconutil` is used for .icns.
Run from the repository root:  python3 scripts/generate-icons.py
"""

import shutil
import struct
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "docs" / "branding" / "logo-mark.svg"
OUT = ROOT / "apps" / "desktop" / "src-tauri" / "icons"


def render(size: int, dest: Path) -> None:
    subprocess.run(
        ["rsvg-convert", "-w", str(size), "-h", str(size), "-o", str(dest), str(SOURCE)],
        check=True,
    )


def write_ico(pngs: list[Path], dest: Path) -> None:
    """ICO container with embedded PNG images (supported since Windows Vista)."""
    images = [p.read_bytes() for p in pngs]
    header = struct.pack("<HHH", 0, 1, len(images))
    offset = 6 + 16 * len(images)
    entries = b""
    for path, data in zip(pngs, images):
        size = int(path.stem.split("-")[-1])
        dim = 0 if size >= 256 else size
        entries += struct.pack("<BBBBHHII", dim, dim, 0, 0, 1, 32, len(data), offset)
        offset += len(data)
    dest.write_bytes(header + entries + b"".join(images))


def main() -> int:
    if shutil.which("rsvg-convert") is None:
        print("rsvg-convert is required", file=sys.stderr)
        return 1
    OUT.mkdir(parents=True, exist_ok=True)
    render(32, OUT / "32x32.png")
    render(128, OUT / "128x128.png")
    render(256, OUT / "128x128@2x.png")
    render(512, OUT / "icon.png")

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        ico_sizes = [16, 24, 32, 48, 64, 128, 256]
        ico_pngs = []
        for s in ico_sizes:
            p = tmp / f"ico-{s}.png"
            render(s, p)
            ico_pngs.append(p)
        write_ico(ico_pngs, OUT / "icon.ico")

        if shutil.which("iconutil"):
            iconset = tmp / "icon.iconset"
            iconset.mkdir()
            for s in [16, 32, 128, 256, 512]:
                render(s, iconset / f"icon_{s}x{s}.png")
                render(s * 2, iconset / f"icon_{s}x{s}@2x.png")
            subprocess.run(["iconutil", "-c", "icns", str(iconset), "-o", str(OUT / "icon.icns")], check=True)
        else:
            print("iconutil not found; icon.icns not regenerated", file=sys.stderr)
    print(f"icons written to {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
