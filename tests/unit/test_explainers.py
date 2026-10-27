import re
import shutil
import subprocess
from pathlib import Path

import pytest

from controlplane.catalog import load_catalog
from controlplane.explainers import render_chaos
from controlplane.topology import load_preset

ROOT = Path(__file__).resolve().parents[2]
CAT = load_catalog(ROOT / "catalog" / "regions.yaml")
KW = dict(default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)
HTML = (ROOT / "web" / "explainers" / "chaos.html").read_text(encoding="utf-8")


def preset(name):
    return load_preset(ROOT / "presets", name, CAT, **KW)


def script_of(html):
    return html.split("<script>", 1)[1].rsplit("</script>", 1)[0]


def test_aws_renders_the_designed_explainer_with_the_preset_name():
    out = render_chaos(HTML, preset("aws"), CAT, "Acme Bank")
    assert out == HTML.replace("{{DISPLAY_NAME}}", "Acme Bank")


@pytest.mark.parametrize("name", ["azure", "gcp", "ibm", "multi-cloud"])
def test_other_clouds_replace_every_aws_region(name):
    topo = preset(name)
    out = render_chaos(HTML, topo, CAT, topo.display_name)
    for old in ("us-east-1", "us-west-2", "eu-west-1", "N. Virginia", "'Oregon'"):
        legit = {r.name for r in topo.regions} | {r.label for r in topo.regions} | {z for r in topo.regions for z in r.zones}
        if not any(old.strip("'") in x for x in legit):
            assert old not in out, (name, old)
    assert topo.primary.name in out and "{{" not in out
    assert re.search(r"const REG = \{\n  \"" + re.escape(topo.primary.name), out)


@pytest.mark.skipif(not shutil.which("node"), reason="node not installed")
@pytest.mark.parametrize("name", ["aws", "azure", "multi-cloud"])
def test_rendered_script_is_valid_javascript(name, tmp_path):
    topo = preset(name)
    f = tmp_path / "x.js"
    f.write_text(script_of(render_chaos(HTML, topo, CAT, topo.display_name)))
    assert subprocess.run(["node", "--check", str(f)], capture_output=True).returncode == 0


def test_without_a_cluster_only_the_name_is_filled_in():
    assert render_chaos(HTML, None, CAT, "X") == HTML.replace("{{DISPLAY_NAME}}", "X")


def test_names_and_labels_are_escaped_for_html_and_javascript(tmp_path):
    from dataclasses import replace
    topo = preset("azure")
    regions = tuple(replace(r, label="O'Hare </script><b>") if r.primary else r for r in topo.regions)
    topo = replace(topo, regions=regions)
    out = render_chaos(HTML, topo, CAT, "<img src=x onerror=alert(1)>")
    assert "<img src=x" not in out and "</script><b>" not in out
    if shutil.which("node"):
        f = tmp_path / "x.js"
        f.write_text(script_of(out))
        assert subprocess.run(["node", "--check", str(f)], capture_output=True).returncode == 0
