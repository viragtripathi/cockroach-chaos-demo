from pathlib import Path

from controlplane.settings import Settings


def test_defaults_match_the_spec():
    s = Settings.from_env({})
    assert s.crdb_version == "v26.3.2"
    assert s.preset == "aws"
    assert s.subnet == "10.77.0.0/16"
    assert s.network == "crdb-chaos"
    assert s.max_nodes == 15
    assert s.haproxy_container == "haproxy"
    assert s.time_until_store_dead == "1m15s"
    assert s.data_dir == Path("/data")
    assert s.presets_dir == Path("/app/presets")
    assert s.crdb_license is None and s.crdb_org is None


def test_environment_overrides_and_blank_values_fall_back():
    s = Settings.from_env({
        "CRDB_VERSION": "v25.4.14", "PRESET": "gcp", "MAX_NODES": "9",
        "CHAOS_SUBNET": "  ", "CRDB_LICENSE": "key", "CRDB_ORG": "Acme",
    })
    assert s.crdb_version == "v25.4.14"
    assert s.preset == "gcp"
    assert s.max_nodes == 9
    assert s.subnet == "10.77.0.0/16"
    assert (s.crdb_license, s.crdb_org) == ("key", "Acme")


def test_tokens_and_helper_image():
    s = Settings.from_env({})
    assert s.control_token is None and s.trigger_token is None
    assert s.helper_image == "crdb-chaos-control-plane:local" and s.panel_port == 8088
    s = Settings.from_env({"CONTROL_TOKEN": "a", "TRIGGER_TOKEN": "b", "PANEL_PORT": "9000"})
    assert (s.control_token, s.trigger_token, s.panel_port) == ("a", "b", 9000)


def test_deployment_model_defaults_to_multi_region():
    assert Settings.from_env({}).model == "multi-region"
    assert Settings.from_env({"DEPLOYMENT_MODEL": "single-region"}).model == "single-region"
