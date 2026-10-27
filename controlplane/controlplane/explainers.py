"""Serve the motion explainers. The chaos explainer is templated from the running cluster so an Azure demo
plays with Azure regions; the default AWS layout maps onto itself, so it renders exactly as designed."""
from __future__ import annotations

import html as htmlmod
import json
import math
import re

from .netem import BASE_RTT_MS, FIBER_KM_PER_MS, ROUTE_FACTOR, _distance_km, rtt_ms
from .topology import Topology

# Region slots the chaos explainer was designed around: primary, survivor, third region, scale-out region.
SLOTS = [
    {"name": "us-east-1", "city": "N. Virginia", "zones": ["us-east-1a", "us-east-1b", "us-east-1c"]},
    {"name": "us-west-2", "city": "Oregon", "zones": ["us-west-2a", "us-west-2b", "us-west-2c"]},
    {"name": "eu-west-1", "city": "Ireland", "zones": ["eu-west-1a", "eu-west-1b", "eu-west-1c"]},
    {"name": "ap-northeast-1", "city": "Tokyo", "zones": ["ap-northeast-1a", "ap-northeast-1c", "ap-northeast-1d"]},
]
ARC_TIMES = {(0, 1): 16.0, (0, 2): 16.35, (1, 2): 16.7, (1, 3): 93.3}
# Offsets (lat, lon) around a region for the "users" that send it traffic.
USER_OFFSETS = [(1.8, 3.4), (3.5, 6.4), (-5.2, -7.0), (3.0, -10.2), (4.8, -2.0), (-1.2, 8.6)]
PROVIDER_LABEL = {"aws": "AWS", "gcp": "Google Cloud", "azure": "Azure", "ibm": "IBM Cloud", "oci": "OCI"}


def _slot_regions(topo: Topology, catalog):
    """Pick four located regions: primary, secondary (or next), another, and a scale-out region."""
    located = [r for r in topo.regions if r.location]
    if len(located) < 3 or not topo.primary.location:
        return None
    first = topo.primary
    second = topo.secondary if topo.secondary and topo.secondary.location else next(r for r in located if r is not first)
    third = next(r for r in located if r not in (first, second))
    rest = [r for r in located if r not in (first, second, third)]
    fourth = None
    if rest:
        r = rest[0]
        fourth = {"name": r.name, "label": r.label, "location": r.location, "zones": list(r.zones)}
    elif catalog is not None:
        have = {r.name for r in topo.regions}
        options = [c for c in catalog.regions_for(first.provider) if c.name not in have]
        if options:
            designed = [c for c in options if c.name == SLOTS[3]["name"]]
            c = designed[0] if designed else max(options, key=lambda c: abs(c.location[1] - first.location[1]))
            fourth = {"name": c.name, "label": c.label, "location": c.location, "zones": list(c.zones)}
    if fourth is None:
        return None
    picked = [{"name": r.name, "label": r.label, "location": r.location, "zones": list(r.zones)} for r in (first, second, third)]
    return picked + [fourth]


def _mean_lon(lons):
    x = sum(math.cos(math.radians(v)) for v in lons)
    y = sum(math.sin(math.radians(v)) for v in lons)
    return math.degrees(math.atan2(y, x))


def _span(lons, center):
    return max(abs(((v - center + 540) % 360) - 180) for v in lons) * 2


def _camera(regions):
    lons3 = [r["location"][1] for r in regions[:3]]
    lats3 = [r["location"][0] for r in regions[:3]]
    c1 = _mean_lon(lons3)
    s1 = min(320, max(150, _span(lons3, c1) * 1.55 + 40))
    lons4 = lons3 + [regions[3]["location"][1]]
    c2 = _mean_lon(lons4)
    s2 = min(340, max(s1 + 40, _span(lons4, c2) * 1.35 + 40))
    lat1 = sum(lats3) / 3
    lat2 = sum(lats3 + [regions[3]["location"][0]]) / 4
    f = lambda v: round(v, 1)
    return (f"const CAMK = [{{ t:0, lon:{f(c1)}, lat:{f(lat1)}, span:{f(s1)} }}, {{ t:89, lon:{f(c1)}, lat:{f(lat1)}, span:{f(s1)} }}, "
            f"{{ t:91.8, lon:{f(c2)}, lat:{f(lat2)}, span:{f(s2)} }}, {{ t:1e9, lon:{f(c2)}, lat:{f(lat2)}, span:{f(s2)} }}];")


def _js(value: str) -> str:
    """A double-quoted JS string literal that is also safe inside an HTML <script> block."""
    return json.dumps(value).replace("<", "\\u003c")


def _js_inner(value: str) -> str:
    """Text to splice inside an existing single-quoted JS string."""
    return _js(value)[1:-1].replace("'", "\\'")


def render_chaos(html: str, topo: Topology | None, catalog, display_name: str) -> str:
    html = html.replace("{{DISPLAY_NAME}}", htmlmod.escape(display_name))
    if topo is None:
        return html
    regions = _slot_regions(topo, catalog)
    if regions is None:
        return html  # fewer than three regions with coordinates: keep the designed layout
    if [r["name"] for r in regions] == [s["name"] for s in SLOTS]:
        return html.replace("$ ./run.sh up --preset aws", f"$ ./run.sh up --preset {topo.preset}")  # the designed layout

    # Names and zones, swapped in one pass so a new name can never be rewritten again.
    mapping: dict[str, str] = {}
    for slot, r in zip(SLOTS, regions):
        zones = (r["zones"] * 3)[:3]
        for old, new in zip(slot["zones"], zones):
            mapping[old] = new
        mapping[slot["name"]] = r["name"]
    pattern = re.compile(r"(?<![a-z0-9-])(" + "|".join(re.escape(k) for k in sorted(mapping, key=len, reverse=True)) + r")(?![a-z0-9])")
    head, sep, script = html.partition("<script>\n'use strict';")
    swap = lambda text: pattern.sub(lambda m: mapping[m.group(1)], text)
    html = swap(head) + sep + swap(script)

    for slot, r in zip(SLOTS, regions):
        html = html.replace(f"'{slot['city']}'", _js(r["label"]))
    html = html.replace("in Tokyo", f"in {_js_inner(regions[3]['label'])}")

    reg = ",\n".join(f"  {_js(r['name'])}: {{ lat: {r['location'][0]}, lon: {r['location'][1]}, city: {_js(r['label'])} }}"
                     for r in regions)
    html = re.sub(r"const REG = \{.*?\n\};", lambda m: "const REG = {\n" + reg + ",\n};", html, count=1, flags=re.S)

    by_slot = {i: next(x for x in topo.regions if x.name == r["name"]) if any(x.name == r["name"] for x in topo.regions) else None
               for i, r in enumerate(regions)}

    def rtt(i, j):
        a, b = by_slot[i], by_slot[j]
        if a and b:
            return round(rtt_ms(a, b, 1.0))
        la, lb = regions[i]["location"], regions[j]["location"]
        return round(2 * _distance_km(la, lb) / FIBER_KM_PER_MS * ROUTE_FACTOR + BASE_RTT_MS)
    arcs = ",".join(f"[{json.dumps(regions[i]['name'])},{json.dumps(regions[j]['name'])},{rtt(i, j)},{t}]".replace('"', "'")
                    for (i, j), t in ARC_TIMES.items())
    html = re.sub(r"const ARCS = \[\[.*?\]\];", lambda m: f"const ARCS = [{arcs}];", html, count=1)

    users = ",".join(f"[{round(r['location'][0] + dy, 1)},{round(r['location'][1] + dx, 1)},'{r['name']}']"
                     for r in regions[:3] for dy, dx in USER_OFFSETS)
    html = re.sub(r"const USERS = \[.*?\n\];", lambda m: f"const USERS = [\n  {users},\n];", html, count=1, flags=re.S)
    html = re.sub(r"const CAMK = \[.*?\];", lambda m: _camera(regions), html, count=1)

    providers = sorted({r.provider for r in topo.regions})
    label = " + ".join(PROVIDER_LABEL.get(p, p.upper()) for p in providers)
    chip = " · ".join(p.upper() for p in providers)
    html = html.replace("Nine nodes on AWS.", f"Nine nodes on {label}.")
    html = html.replace("Production · AWS · 3 regions", f"Production · {chip} · 3 regions")
    html = html.replace("--locality=cloud=aws,", f"--locality=cloud={topo.primary.provider},")
    html = html.replace("$ ./run.sh up --preset aws", f"$ ./run.sh up --preset {topo.preset}")
    return html
