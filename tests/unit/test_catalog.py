from pathlib import Path

from controlplane.catalog import load_catalog, parse_catalog

ROOT = Path(__file__).resolve().parents[2]


def test_lookup_returns_label_location_and_zones():
    cat = parse_catalog({"providers": {"gcp": {"us-east1": {
        "label": "South Carolina", "location": [33.2, -80.0],
        "zones": ["us-east1-b", "us-east1-c", "us-east1-d"]}}}})
    r = cat.lookup("gcp", "us-east1")
    assert r.label == "South Carolina"
    assert r.location == (33.2, -80.0)
    assert r.zones == ("us-east1-b", "us-east1-c", "us-east1-d")
    assert cat.lookup("gcp", "nowhere1") is None
    assert cat.lookup("aws", "us-east1") is None


def test_shipped_catalog_covers_every_region_used_by_presets():
    cat = load_catalog(ROOT / "catalog" / "regions.yaml")
    assert {"aws", "gcp", "azure", "ibm", "oci"} <= set(cat.providers())
    for provider, region in [
        ("aws", "us-east-1"), ("aws", "us-west-2"), ("aws", "eu-west-1"), ("aws", "us-east-2"),
        ("gcp", "us-east1"), ("gcp", "us-central1"), ("gcp", "europe-west1"),
        ("azure", "eastus"), ("azure", "westus2"), ("azure", "westeurope"),
        ("ibm", "us-south"), ("ibm", "us-east"), ("ibm", "eu-de"),
    ]:
        entry = cat.lookup(provider, region)
        assert entry is not None, (provider, region)
        assert len(entry.zones) >= 2
        lat, lon = entry.location
        assert -90 <= lat <= 90 and -180 <= lon <= 180


def test_regions_for_lists_a_providers_regions_sorted():
    cat = load_catalog(ROOT / "catalog" / "regions.yaml")
    names = [r.name for r in cat.regions_for("azure")]
    assert names == sorted(names) and "eastus" in names
