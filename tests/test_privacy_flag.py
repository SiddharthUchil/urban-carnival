"""The `pseudonymize` run flag (ADR-0007) -- default and parsing, for both products.

Identity pseudonymization is the one privacy control in the pipeline, and it is a per-run
job parameter rather than a code constant so that turning it off is visible in the job
config rather than buried in a diff. That design only holds if the DEFAULT stays protective:
these tests fail if someone flips the default, and pin the deliberately-asymmetric parsing
(only explicit false tokens turn it off, so a typo lands on the safe branch).

Gold is unaffected either way -- silver_lib.pseudonymize_expr is a deterministic keyed hash,
so every distinct count is identical. This is a privacy contract, not a metric contract.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "databricks"))

from conf import coverme_settings as cm  # noqa: E402
from conf import settings as gw  # noqa: E402

MODULES = [pytest.param(gw, id="gwam"), pytest.param(cm, id="coverme")]

FALSE_TOKENS = ["false", "False", "FALSE", " false ", "0", "no", "No"]
# Not false: unrecognised input must land on the protective branch.
NOT_FALSE_TOKENS = ["true", "True", "1", "yes", "flase", "off", "disabled"]


class _FakeWidgets:
    def __init__(self, values):
        self._v = dict(values)

    def text(self, name, default):
        self._v.setdefault(name, default)

    def get(self, name):
        return self._v[name]


class _FakeDbutils:
    """Enough of dbutils for conf._widget: .widgets.text(name, default) then .widgets.get(name)."""

    def __init__(self, **values):
        self.widgets = _FakeWidgets(values)


@pytest.mark.parametrize("mod", MODULES)
def test_default_is_protective(mod):
    assert mod.PSEUDONYMIZE_DEFAULT is True, (
        "pseudonymization must default ON. Turning it off is a per-run decision "
        "(pseudonymize=false), never the code's opinion -- ADR-0007."
    )


@pytest.mark.parametrize("mod", MODULES)
def test_resolve_defaults_to_on(mod):
    s = mod.resolve(_FakeDbutils(target_catalog="usdo_aa_catalog"))
    assert s.pseudonymize is True


@pytest.mark.parametrize("mod", MODULES)
@pytest.mark.parametrize("token", FALSE_TOKENS)
def test_explicit_false_tokens_turn_it_off(mod, token):
    s = mod.resolve(_FakeDbutils(target_catalog="usdo_aa_catalog", pseudonymize=token))
    assert s.pseudonymize is False


@pytest.mark.parametrize("mod", MODULES)
@pytest.mark.parametrize("token", NOT_FALSE_TOKENS)
def test_anything_else_stays_on(mod, token):
    s = mod.resolve(_FakeDbutils(target_catalog="usdo_aa_catalog", pseudonymize=token))
    assert s.pseudonymize is True, (
        f"{token!r} must not disable pseudonymization -- a typo has to fail safe."
    )


@pytest.mark.parametrize("mod", MODULES)
def test_identity_cols_are_the_three_adr_0007_fields(mod):
    assert mod.IDENTITY_COLS == ["mcvisid", "post_visid_high", "post_visid_low"]
