"""metric-registry.yaml <-> detect/cm_registry.py governance contract.

research/claude/metric-registry.yaml is the governed source of truth (v0.3.0 records
Kerrian's SME rulings of 2026-07-27; v0.4.0 adds the GWAM four-channel scope seed; v0.5.0
records the 2026-07-29 rulings -- GWAM narrowed to the Public Website channel, so 14 of the
17 GWAM entries are deferred and 2 anomaly-signal seeds are added, and CoverMe's language
rule is ratified in meta with no series change; v0.6.0 records the 2026-07-30 extended-probe
evidence -- C3/C11/C12 -- and closes doc 19 gate G2, again with no series change);
detect/cm_registry.py is its Python binding. Enforces the yaml's own validation_rules
block plus the code pin, so REGISTRY_VERSION and the copied status/direction/owner
governance fields cannot drift silently.

The CoverMe assertions walk SHEET_COUNTS only, so the GWAM section is deliberately
outside them -- it is a scope transcription, not a dictionary extraction, and it has
its own guard in test_gwam_channel_seed_counts.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "detect"))

yaml = pytest.importorskip("yaml")

from cm_registry import EVENT_METRICS, REGISTRY_VERSION, SERIES  # noqa: E402

FUNNEL_METRIC_IDS = {
    "pel_228_quote_start", "pel_229_quote_complete", "pel_232_save_quote",
    "pel_269_app_start", "pel_240_app_confirm",
}
SHEET_COUNTS = {"data_feed_columns": 9, "post_eVar": 8, "post_event_list": 12}


@pytest.fixture(scope="module")
def registry():
    path = REPO / "research" / "claude" / "metric-registry.yaml"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def entries(registry):
    return [e for sheet in SHEET_COUNTS for e in registry[sheet]]


def test_version_pin(registry):
    assert str(registry["meta"]["version"]) == REGISTRY_VERSION == "0.6.0"


def test_per_sheet_counts(registry):
    for sheet, want in SHEET_COUNTS.items():
        assert len(registry[sheet]) == want, f"{sheet}: seed count {want}"


def test_metric_ids_unique(entries):
    ids = [e["metric_id"] for e in entries]
    assert len(ids) == len(set(ids)) == 29


def test_enums_valid(entries):
    for e in entries:
        assert e["status"] in {"active", "candidate", "deferred"}, e["metric_id"]
        assert e["direction"] in {"higher_is_good", "higher_is_bad",
                                  "context_dependent"}, e["metric_id"]
        assert e["grain"] in {"daily", "hourly"}, e["metric_id"]


def test_active_set_is_exactly_the_funnel(entries):
    active = {e["metric_id"] for e in entries if e["status"] == "active"}
    assert active == FUNNEL_METRIC_IDS


def test_event_metric_ids_match_yaml(registry):
    yaml_ids = {e["metric_id"] for e in registry["post_event_list"]}
    code_ids = {mid for mid, _ in EVENT_METRICS.values()}
    assert code_ids == yaml_ids


def test_series_governance_matches_yaml(registry):
    by_id = {e["metric_id"]: e for e in registry["post_event_list"]}
    specs = {s.metric_id: s for s in SERIES}
    for mid, e in by_id.items():
        s = specs[mid]
        assert (s.status, s.direction, s.owner) == \
            (e["status"], e["direction"], e["owner"]), mid


# --- GWAM channel scope seed (v0.4.0 transcription, v0.5.0 single-channel ruling) ---------
GWAM_CHANNELS = {"public_website", "web_member", "mobile", "manulifeid"}
GWAM_IN_SCOPE = {"public_website"}      # SME ruling 2026-07-29: the only alerting channel


@pytest.fixture(scope="module")
def gwam(registry):
    return registry["gwam_channel_metrics"]


def test_gwam_channel_seed_counts(gwam):
    """19 = the 17 transcribed pairs (3 traffic x 4 channels, + errors on 3, + 2 sign-in on
    ManulifeID) plus the 2 public-website anomaly-signal seeds from the 2026-07-29 update."""
    assert len(gwam) == 19
    ids = [e["metric_id"] for e in gwam]
    assert len(ids) == len(set(ids))
    by_channel = {c: sum(1 for e in gwam if e["channel"] == c) for c in GWAM_CHANNELS}
    assert by_channel == {"public_website": 5, "web_member": 4, "mobile": 4, "manulifeid": 6}


def test_gwam_status_partition(gwam):
    """The 2026-07-29 ruling narrowed alerting to the Public Website channel. Those entries stay
    `candidate` -- nothing may reach `active` before doc 20 Q3b (the wealth-ca / pvt-wealth brand
    variants) and Q6 (the page-view numerator) are answered. The other three channels are
    `deferred`, which means kept-with-evidence: their predicates and probe findings survive so a
    re-widening is a status flip. Deferring rather than deleting is also what keeps the dissolved
    D8 conflict visible -- it was never ruled on, so it returns if a signed-in channel comes back.
    """
    for e in gwam:
        want = "candidate" if e["channel"] in GWAM_IN_SCOPE else "deferred"
        assert e["status"] == want, f'{e["metric_id"]} ({e["channel"]}) should be {want}'
    assert {e["owner"] for e in gwam} == {"TBD"}


def test_gwam_enums_and_domain(gwam):
    for e in gwam:
        assert e["domain"] == "gwam_retirement", e["metric_id"]
        assert e["channel"] in GWAM_CHANNELS, e["metric_id"]
        assert e["direction"] in {"higher_is_good", "higher_is_bad",
                                  "context_dependent"}, e["metric_id"]
        assert e["grain"] in {"daily", "hourly"}, e["metric_id"]


def test_gwam_ids_disjoint_from_coverme(entries, gwam):
    """The two products share one file; a metric_id collision would silently overwrite one."""
    assert not ({e["metric_id"] for e in gwam} & {e["metric_id"] for e in entries})
