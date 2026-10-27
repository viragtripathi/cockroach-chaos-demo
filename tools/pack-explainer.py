#!/usr/bin/env python3
"""Pack an explainer into one self-contained HTML file: fonts, logo, narration and music embedded.

    tools/pack-explainer.py chaos --out chaos-explainer.html

The result opens from a laptop, a USB stick or a Slack download with no server and no internet, and keeps every
control (voice, theme, loop, scrubbing, chapter keys). It is the same page the panel serves; the panel's /static/
URLs are swapped for embedded data.
"""
from __future__ import annotations

import argparse
import base64
import json
import mimetypes
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / "web"


def data_url(path: Path) -> str:
    mime = {".woff2": "font/woff2", ".svg": "image/svg+xml", ".mp3": "audio/mpeg", ".ico": "image/x-icon"}.get(
        path.suffix, mimetypes.guess_type(path.name)[0] or "application/octet-stream")
    return f"data:{mime};base64,{base64.b64encode(path.read_bytes()).decode()}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("name", choices=["continuum", "chaos"])
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--no-audio", action="store_true", help="leave narration and music out (smaller file)")
    args = parser.parse_args()

    src = WEB / "explainers" / f"{args.name}.html"
    html = src.read_text(encoding="utf-8")

    # fonts: inline the stylesheet with the font files as data URLs
    fonts_css = (WEB / "shared" / "fonts.css").read_text(encoding="utf-8")
    fonts_css = re.sub(r'url\("/static/shared/fonts/([^"]+)"\)',
                       lambda m: f'url("{data_url(WEB / "shared" / "fonts" / m.group(1))}")', fonts_css)
    html = html.replace('<link rel="stylesheet" href="/static/shared/fonts.css">', f"<style>\n{fonts_css}\n</style>")
    html = html.replace('<link rel="icon" href="/static/shared/favicon.ico">',
                        f'<link rel="icon" href="{data_url(WEB / "shared" / "favicon.ico")}">')

    embed = {"crl-logo.svg": data_url(WEB / "shared" / "crl-logo.svg")}
    audio = WEB / "explainers" / "audio"
    manifest = audio / f"{args.name}-narration.json"
    if not args.no_audio and manifest.is_file():
        narration = json.loads(manifest.read_text(encoding="utf-8"))
        embed["narration"] = narration
        for voice in narration["voices"].values():
            for ch in voice["chapters"].values():
                embed["audio/" + ch["file"]] = data_url(audio / ch["file"])
        underscore = audio / f"{args.name}-underscore.mp3"
        if underscore.is_file():
            embed["audio/" + underscore.name] = data_url(underscore)
    # land mask for the chaos explainer
    html = html.replace('<script src="/static/explainers/landmask.js"></script>',
                        "<script>\n" + (WEB / "explainers" / "landmask.js").read_text(encoding="utf-8") + "\n</script>"
                        if (WEB / "explainers" / "landmask.js").is_file() else "")

    marker = "<script>\n'use strict';"
    if marker not in html:
        sys.exit("unexpected explainer layout: script marker not found")
    html = html.replace(marker, "<script>\nwindow.EMBED = " + json.dumps(embed) + ";\n</script>\n" + marker, 1)
    for leftover in re.findall(r'["\'(]/static/[^"\')]+', html):
        if "AUDIO_BASE" not in leftover and "crl-logo" not in leftover:
            print(f"note: still references {leftover.strip(chr(34) + chr(39) + '(')}", file=sys.stderr)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html, encoding="utf-8")
    print(f"wrote {args.out} ({args.out.stat().st_size / 1_048_576:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
