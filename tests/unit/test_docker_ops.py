import json
import subprocess
from pathlib import Path

import pytest

from controlplane.catalog import load_catalog
from controlplane.docker_ops import Docker, DockerError, node_run_args, parse_inspect
from controlplane.topology import load_preset

ROOT = Path(__file__).resolve().parents[2]
TOPO = load_preset(ROOT / "presets", "classic", load_catalog(ROOT / "catalog" / "regions.yaml"),
                   default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)


class FakeRunner:
    def __init__(self, responses):
        self.responses = responses  # list of (returncode, stdout, stderr) consumed in order
        self.calls = []

    def __call__(self, args, timeout):
        self.calls.append(list(args))
        code, out, err = self.responses.pop(0)
        return subprocess.CompletedProcess(args, code, out, err)


def test_node_run_args_carry_identity_locality_and_limits():
    spec = TOPO.nodes[0]
    args = node_run_args(spec, TOPO, ["us-east-1a-n1:26257", "us-east-1b-n2:26257"], "crdb-chaos")
    assert args[:12] == ["run", "-d", "--name", "us-east-1a-n1", "--hostname", "us-east-1a-n1",
                         "--network", "crdb-chaos", "--ip", "10.77.1.11", "--label", "crdb-chaos.node=1"]
    assert "crdb-chaos.region=us-east-1" in args and "crdb-chaos.ip=10.77.1.11" in args
    i = args.index("docker.io/cockroachdb/cockroach:v26.3.2")
    assert args[i + 1:i + 3] == ["start", "--insecure"]
    assert "--advertise-addr=us-east-1a-n1:26257" in args
    assert "--locality=cloud=aws,region=us-east-1,zone=us-east-1a" in args
    assert "--cluster-name=global-payments" in args
    assert "--join=us-east-1a-n1:26257,us-east-1b-n2:26257" in args
    assert "--cache=256MiB" in args and "--max-sql-memory=256MiB" in args


def test_parse_inspect_handles_leading_slash_and_missing_networks():
    raw = json.dumps([
        {"Name": "/us-east-1a-n1", "Id": "abc", "State": {"Running": True, "Status": "running"},
         "Config": {"Labels": {"crdb-chaos.node": "1"}},
         "NetworkSettings": {"Networks": {"crdb-chaos": {"IPAddress": "10.77.1.11"}}}},
        {"Name": "us-east-1b-n2", "Id": "def", "State": {"Running": False, "Status": "exited"},
         "Config": {"Labels": None}, "NetworkSettings": {}},
    ])
    a, b = parse_inspect(raw)
    assert (a.name, a.running, a.ips) == ("us-east-1a-n1", True, {"crdb-chaos": "10.77.1.11"})
    assert (b.name, b.running, b.status, b.labels, b.ips) == ("us-east-1b-n2", False, "exited", {}, {})


def test_list_nodes_filters_by_label_then_inspects():
    inspect = json.dumps([{"Name": "/n1", "Id": "1", "State": {"Running": True}, "Config": {"Labels": {}}, "NetworkSettings": {"Networks": {}}}])
    run = FakeRunner([(0, "n1\n", ""), (0, inspect, "")])
    nodes = Docker(run).list_nodes()
    assert [n.name for n in nodes] == ["n1"]
    assert run.calls[0] == ["ps", "-a", "--filter", "label=crdb-chaos.node=1", "--format", "{{.Names}}"]
    assert run.calls[1] == ["inspect", "n1"]


def test_list_nodes_with_none_skips_inspect():
    run = FakeRunner([(0, "", "")])
    assert Docker(run).list_nodes() == []
    assert len(run.calls) == 1


def test_failures_raise_with_stderr_and_timeouts_are_reported():
    with pytest.raises(DockerError, match="no such image"):
        Docker(FakeRunner([(1, "", "no such image")])).pull("x")

    def slow(args, timeout):
        raise subprocess.TimeoutExpired(args, timeout)

    with pytest.raises(DockerError, match="timed out"):
        Docker(slow).pull("x")


def test_remove_tolerates_missing_containers_and_image_present_is_boolean():
    d = Docker(FakeRunner([(1, "", "no such container"), (1, "", "missing"), (0, "[]", "")]))
    d.remove("ghost")
    assert d.image_present("a") is False
    assert d.image_present("b") is True


def test_chaos_primitives_issue_the_right_commands():
    run = FakeRunner([(0, "", "")] * 5)
    d = Docker(run)
    d.stop("n1")
    d.start("n1")
    d.network_disconnect("crdb-chaos", "n1")
    d.network_connect("crdb-chaos", "n1", "10.77.1.11")
    d.run_helper("n1", "tc qdisc show", image="img:local")
    assert run.calls == [
        ["stop", "-t", "20", "n1"],
        ["start", "n1"],
        ["network", "disconnect", "crdb-chaos", "n1"],
        ["network", "connect", "--ip", "10.77.1.11", "crdb-chaos", "n1"],
        ["run", "--rm", "--network", "container:n1", "--cap-add", "NET_ADMIN", "--entrypoint", "sh", "img:local", "-c", "tc qdisc show"],
    ]


def test_nodes_get_a_go_memory_limit_and_bounded_logs():
    spec = TOPO.nodes[0]
    args = node_run_args(spec, TOPO, ["a:26257"], "crdb-chaos")
    assert "--max-go-memory=1GiB" in args  # derived default (2.25 x sql memory) is too small: idle nodes GC nonstop
    log = next(a for a in args if a.startswith("--log="))
    assert "max-group-size" in log and "max-file-size" in log


def test_parse_inspect_reports_exit_codes():
    raw = json.dumps([{"Name": "n1", "Id": "1", "State": {"Running": False, "Status": "exited", "ExitCode": 7},
                       "Config": {"Labels": {}}, "NetworkSettings": {}}])
    assert parse_inspect(raw)[0].exit_code == 7
