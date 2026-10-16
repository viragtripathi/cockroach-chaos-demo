import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STAGE = [ROOT / "web" / "stage" / name for name in ("index.html", "stage.js", "stage.css")]
FORBIDDEN = re.compile(r"\b(docker|container|podman|localhost)\b", re.IGNORECASE)


def test_stage_never_mentions_local_infrastructure():
    for path in STAGE:
        hits = [m.group(0) for m in FORBIDDEN.finditer(path.read_text(encoding="utf-8"))]
        assert not hits, f"{path.name} mentions {hits}"


def test_stage_uses_shared_brand_tokens_and_live_events():
    html = (ROOT / "web" / "stage" / "index.html").read_text(encoding="utf-8")
    js = (ROOT / "web" / "stage" / "stage.js").read_text(encoding="utf-8")
    tokens = (ROOT / "web" / "shared" / "tokens.css").read_text(encoding="utf-8")
    assert "/static/shared/tokens.css" in html and "/static/stage/stage.js" in html
    assert "EventSource('/api/events')" in js
    for token in ("#aede54", "#9059ff", "#60ecff", "#f9f8f3", "[data-theme=light]", "Geist-Variable.woff2"):
        assert token in tokens
    for asset in ("fonts/Geist-Variable.woff2", "fonts/GeistMono-Variable.woff2", "fonts/OFL.txt", "crl-logo.svg", "favicon.ico"):
        assert (ROOT / "web" / "shared" / asset).is_file(), asset


def test_customer_name_comes_from_presets_not_code():
    for folder in ("controlplane", "web"):
        for path in (ROOT / folder).rglob("*"):
            if path.suffix in (".py", ".js", ".html", ".css"):
                assert "Global Payments" not in path.read_text(encoding="utf-8"), path


def test_shared_tooltip_wording_follows_the_stage_rules():
    for name in ("glossary.js", "tips.js"):
        text = (ROOT / "web" / "shared" / name).read_text(encoding="utf-8")
        hits = [m.group(0) for m in FORBIDDEN.finditer(text)]
        assert not hits, f"{name} mentions {hits}"


def test_every_stage_button_and_presenter_action_has_an_explanation():
    glossary = (ROOT / "web" / "shared" / "glossary.js").read_text(encoding="utf-8")
    actions = glossary[glossary.index("action: {"):glossary.index("chart:")]
    stage = (ROOT / "web" / "stage" / "stage.js").read_text(encoding="utf-8")
    labels = set(re.findall(r"button\('([^']+)'", stage))
    assert labels, "no region buttons found"
    for label in labels:
        assert f"'{label}':" in actions or f"{label}:" in actions, f"stage button {label!r} has no tooltip"
    presenter = (ROOT / "web" / "presenter" / "index.html").read_text(encoding="utf-8")
    for action in re.findall(r'data-a="([a-z]+)"', presenter):
        assert f"{action}:" in actions, f"presenter action {action!r} has no tooltip"
