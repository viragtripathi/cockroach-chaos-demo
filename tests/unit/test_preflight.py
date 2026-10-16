import json
import subprocess

import preflight
from preflight import Check, main, preset_nodes, run_checks

BASE = dict(nodes=9, subnet="10.77.0.0/16", panel_port=8088)


def fake_docker(ps="", networks=None, mem=16 * 1024**3, info_rc=0, info=None):
    networks = networks or []
    info = info if info is not None else {"MemTotal": mem}  # Docker CLI shape; Podman nests host.memTotal

    def run(args):
        if args[0] == "info":
            assert args == ["info", "--format", "json"]
            return subprocess.CompletedProcess(args, info_rc, json.dumps(info), "")
        if args[0] == "ps":
            return subprocess.CompletedProcess(args, 0, ps, "")
        if args[:2] == ["network", "ls"]:
            return subprocess.CompletedProcess(args, 0, "\n".join(n.get("Id") or n.get("id") for n in networks), "")
        if args[:2] == ["network", "inspect"]:
            return subprocess.CompletedProcess(args, 0, json.dumps(networks), "")
        raise AssertionError(args)
    return run


def by_name(checks):
    return {c.name: c for c in checks}


def test_clean_machine_passes():
    checks = by_name(run_checks(**BASE, docker=fake_docker(), in_use=lambda p: False))
    assert all(c.ok for c in checks.values())


def test_legacy_stack_is_detected_with_the_fix():
    ps = "crdb-e1a\tcockroach-chaos-demo\nhaproxy\tcockroach-chaos-demo\nchaos-backend\tcockroach-chaos-demo\n"
    c = by_name(run_checks(**BASE, docker=fake_docker(ps=ps), in_use=lambda p: True))
    assert not c["legacy"].ok
    assert "./run.sh down --legacy" in c["legacy"].message and "crdb-e1a" in c["legacy"].message
    assert "ports" not in c


def test_our_own_haproxy_is_not_legacy():
    ps = "haproxy\tcrdb-chaos\ncrdb-chaos-control-plane\tcrdb-chaos\n"
    c = by_name(run_checks(**BASE, docker=fake_docker(ps=ps), in_use=lambda p: True))
    assert c["legacy"].ok and "ports" not in c


def test_busy_ports_are_named():
    c = by_name(run_checks(**BASE, docker=fake_docker(), in_use=lambda p: p in (8080, 8088)))
    assert not c["ports"].ok and "8080, 8088" in c["ports"].message


def test_overlapping_subnet_names_the_network_and_suggests_another():
    nets = [{"Id": "n1", "Name": "vpn-bridge", "IPAM": {"Config": [{"Subnet": "10.77.4.0/24"}]}},
            {"Id": "n2", "Name": "crdb-chaos", "IPAM": {"Config": [{"Subnet": "10.77.0.0/16"}]}}]
    c = by_name(run_checks(**BASE, docker=fake_docker(networks=nets), in_use=lambda p: False))
    assert not c["subnet"].ok
    assert "vpn-bridge (10.77.4.0/24)" in c["subnet"].message and "CHAOS_SUBNET=" in c["subnet"].message


def test_low_memory_is_a_warning_not_an_error():
    c = by_name(run_checks(**BASE, docker=fake_docker(mem=2 * 1024**3), in_use=lambda p: False))
    assert not c["memory"].ok and c["memory"].level == "warn" and "--preset small" in c["memory"].message


def test_unreachable_runtime_stops_early():
    checks = run_checks(**BASE, docker=fake_docker(info_rc=1), in_use=lambda p: False)
    assert len(checks) == 1 and not checks[0].ok and "not reachable" in checks[0].message


def test_preset_nodes_sums_block_and_flow_styles(tmp_path):
    p = tmp_path / "p.yaml"
    p.write_text("regions:\n  - { name: a1, nodes: 3 }\n  - name: b1\n    nodes: 2\n", encoding="utf-8")
    assert preset_nodes(p) == 5


def test_main_exits_nonzero_only_on_errors(tmp_path, monkeypatch):
    p = tmp_path / "p.yaml"
    p.write_text("regions:\n  - { name: a1, nodes: 1 }\n", encoding="utf-8")
    args = ["--preset-file", str(p), "--subnet", "10.77.0.0/16", "--panel-port", "8088"]
    monkeypatch.setattr(preflight, "run_checks", lambda **kw: [Check("memory", False, "low", level="warn")])
    assert main(args) == 0
    monkeypatch.setattr(preflight, "run_checks", lambda **kw: [Check("ports", False, "busy")])
    assert main(args) == 1


def test_podman_cli_shapes_for_memory_and_networks_are_understood():
    nets = [{"id": "p1", "name": "podman-bridge", "subnets": [{"subnet": "10.77.9.0/24", "gateway": "10.77.9.1"}]}]
    c = by_name(run_checks(**BASE, docker=fake_docker(networks=nets, info={"host": {"memTotal": 2 * 1024**3}}), in_use=lambda p: False))
    assert c["runtime"].ok
    assert not c["subnet"].ok and "podman-bridge (10.77.9.0/24)" in c["subnet"].message
    assert not c["memory"].ok and "2048 MiB" in c["memory"].message


def test_suggested_subnet_is_actually_free():
    import ipaddress, re
    nets = [{"id": "p0", "name": "podman", "subnets": [{"subnet": "10.88.0.0/16"}]},
            {"Id": "n1", "Name": "vpn-bridge", "IPAM": {"Config": [{"Subnet": "10.77.4.0/24"}]}},
            {"Id": "n2", "Name": "other", "IPAM": {"Config": [{"Subnet": "10.78.0.0/16"}]}}]
    c = by_name(run_checks(**BASE, docker=fake_docker(networks=nets), in_use=lambda p: False))
    suggested = ipaddress.ip_network(re.search(r"CHAOS_SUBNET=(\S+)", c["subnet"].message).group(1))
    assert suggested.prefixlen == 16
    for cidr in ("10.88.0.0/16", "10.77.4.0/24", "10.78.0.0/16", "10.77.0.0/16"):
        assert not suggested.overlaps(ipaddress.ip_network(cidr)), cidr


def test_unreadable_network_output_is_a_warning_not_a_crash():
    base = fake_docker(networks=[{"Id": "abc", "Name": "x"}])

    def run(args):
        if args[:2] == ["network", "inspect"]:
            return subprocess.CompletedProcess(args, 0, "not json", "")
        return base(args)
    checks = by_name(run_checks(**BASE, docker=run, in_use=lambda p: False))
    assert checks["subnet"].ok and "could not" in checks["subnet"].message.lower()


def test_preset_nodes_ignores_comments(tmp_path):
    p = tmp_path / "x.yaml"
    p.write_text("# nodes: 50 would be too many\nregions:\n  - { name: a, nodes: 3 }  # nodes: 9\n", encoding="utf-8")
    assert preset_nodes(p) == 3


def test_single_region_model_estimates_one_region(tmp_path, monkeypatch):
    p = tmp_path / "p.yaml"
    p.write_text("regions:\n  - { name: a, nodes: 2, primary: true }\n  - { name: b, nodes: 2 }\n  - { name: c, nodes: 2 }\n", encoding="utf-8")
    seen = {}
    monkeypatch.setattr(preflight, "run_checks", lambda **kw: seen.update(kw) or [])
    main(["--preset-file", str(p), "--subnet", "10.77.0.0/16", "--panel-port", "8088", "--model", "single-region"])
    assert seen["nodes"] == 3   # the primary region, at least one node per zone
    main(["--preset-file", str(p), "--subnet", "10.77.0.0/16", "--panel-port", "8088"])
    assert seen["nodes"] == 6
