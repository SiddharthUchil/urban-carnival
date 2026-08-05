"""Pin the SME link-rule table in eda/gwam_channel_discovery.py (2026-08-04 scope, doc-16 D13).

Sixteen hand-transcribed URLs are the most fragile artefact in that probe: a single character
edited into any `href` can silently make two rules match the same hit, and the probe would still
run clean and emit a plausible-looking table. These tests fail instead.

The probe calls `dbutils` at module level so it cannot be imported. The rule table is a pure
literal, so it is lifted out with `ast.literal_eval` -- that reads what is actually committed,
which is the point: a scratch script re-typing the URLs would verify nothing.

The collision facts asserted here are quoted in the SME_LINK_RULES comment block and in
research/claude/21-gwam-link-rule-scope.md. If a rule legitimately changes, update all three.
"""
from __future__ import annotations

import ast
import io
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
PROBE = REPO / "eda" / "gwam_channel_discovery.py"
sys.path.insert(0, str(REPO / "databricks"))

from conf.settings import SCOPE_LOGIN_HOST_EXCLUDE  # noqa: E402

FIELDS = {"rule_id", "rule_name", "lang", "link_name", "link_name_mojibake", "href",
          "token", "anti_token", "lang_token", "lang_anti_token"}


def _literal(name):
    """Lift a module-level literal assignment out of the probe without importing it."""
    tree = ast.parse(io.open(PROBE, encoding="utf-8").read())
    for node in tree.body:
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", None) == name:
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {PROBE.name}")


@pytest.fixture(scope="module")
def rules():
    return _literal("SME_LINK_RULES")


@pytest.fixture(scope="module")
def by_key(rules):
    return {f"{r['rule_id']}_{r['lang']}": r["href"] for r in rules}


def _decode(u):
    u = u.lower()
    for enc, dec in _literal("PCT_DECODE"):
        u = u.replace(enc, dec)
    return u


def _matches(rule, href):
    """The probe's rule_match_expr, in Python. token/anti on the DECODED value, language on the
    RAW value -- decoding first would turn the FR sponsor/advisor hrefs, which carry an inner
    `ui_locales%3Den-CA`, into false English matches."""
    raw, dec = href.lower(), _decode(href)
    if rule["token"] not in dec:
        return False
    if rule["anti_token"] and rule["anti_token"] in dec:
        return False
    if rule["lang_token"] and rule["lang_token"] not in raw:
        return False
    if rule["lang_anti_token"] and rule["lang_anti_token"] in raw:
        return False
    return True


# --------------------------------------------------------------------------- structure

def test_eight_rules_each_in_both_languages(rules):
    assert len(rules) == 16, "expected 8 SME rules x {en, fr}"
    ids = sorted({r["rule_id"] for r in rules})
    assert len(ids) == 8, f"expected 8 distinct rule_ids, got {ids}"
    for rid in ids:
        langs = sorted(r["lang"] for r in rules if r["rule_id"] == rid)
        assert langs == ["en", "fr"], f"{rid} is missing a language: {langs}"
    keys = [f"{r['rule_id']}_{r['lang']}" for r in rules]
    assert len(set(keys)) == 16, "duplicate (rule_id, lang) key"


def test_every_rule_carries_the_full_field_set(rules):
    for r in rules:
        assert set(r) == FIELDS, f"{r.get('rule_id')}: {sorted(set(r) ^ FIELDS)}"
        assert r["lang"] in ("en", "fr")
        assert r["href"].startswith("https://"), r["rule_id"]
        assert r["token"], f"{r['rule_id']}/{r['lang']} has no token to match on"


def test_french_accented_names_carry_both_encodings(rules):
    """The SME's file arrived mojibaked (UTF-8 read as Latin-1). We keep the corrected form AND
    the mangled one, because the tag on the site may be mangled too and we must not guess."""
    moj = {f"{r['rule_id']}_{r['lang']}": r for r in rules if r["link_name_mojibake"]}
    assert set(moj) == {"signup_join_fr", "signin_join_fr"}, sorted(moj)
    assert moj["signup_join_fr"]["link_name"] == "créez-en un pour vous inscrire"
    assert moj["signup_join_fr"]["link_name_mojibake"] == "crÃ©ez-en un pour vous inscrire"
    assert moj["signin_join_fr"]["link_name"] == "Ouvrir une session pour adhérer"
    assert moj["signin_join_fr"]["link_name_mojibake"] == "Ouvrir une session pour adhÃ©rer"


def test_app_download_rules_have_a_blank_link_name_in_both_languages(rules):
    """doc-20 Q18. Blank in BOTH languages means evar193 cannot split their EN/FR at all --
    the href query string is the only discriminator, so query-stripping erases them."""
    for r in rules:
        if r["rule_id"] in ("app_apple", "app_android"):
            assert r["link_name"] == "", f"{r['rule_id']}/{r['lang']} gained a link name"


# --------------------------------------------------------------- the matching contract

def test_match_predicate_resolves_every_rule_uniquely(rules):
    """The contract the whole probe rests on: token AND NOT anti_token (decoded) AND the
    language marker (raw) selects exactly one of the 16 for each spec URL."""
    for r in rules:
        key = f"{r['rule_id']}_{r['lang']}"
        hits = sorted(f"{o['rule_id']}_{o['lang']}" for o in rules if _matches(o, r["href"]))
        assert hits == [key], f"{key} also matched {[h for h in hits if h != key]}"


def test_anti_tokens_are_load_bearing(rules):
    """Both anti_tokens guard a real collision. Drop either and the suite above breaks -- this
    names which collision, so a future reader does not 'simplify' them away."""
    by = {f"{r['rule_id']}_{r['lang']}": dict(r) for r in rules}

    naked = dict(by["signin_join_en"], anti_token=None)
    assert _matches(naked, by["signup_join_en"]["href"]), (
        "signin_join's anti_token '/register' is guarding a real overlap: signup_join's href "
        "also contains 'enrolment/handlelogin'")

    naked = dict(by["signin_member_fr"], anti_token=None)
    assert _matches(naked, by["signin_join_fr"]["href"]), (
        "signin_member/fr's anti_token 'goto=' is guarding a real overlap: its href is a strict "
        "prefix of signin_join/fr's")


def test_language_marker_reads_the_raw_href_not_the_decoded_one(rules):
    """doc-20 Q16. The FR sponsor/advisor hrefs carry an inner `ui_locales%3Den-CA`. Decoding
    before reading the language marker turns %3D into '=' and reads them as English."""
    by = {f"{r['rule_id']}_{r['lang']}": r for r in rules}
    for key in ("signin_sponsor_fr", "signin_advisor_fr"):
        href = by[key]["href"]
        assert "ui_locales%3Den-CA" in href, f"{key} no longer carries the inner EN locale"
        assert "ui_locales=en-ca" not in href.lower(), "raw href must not read as English"
        assert "ui_locales=en-ca" in _decode(href), "decoded href does read as English -- the trap"
        assert _matches(by[key], href), f"{key} must still match itself"


# ------------------------------------------------------- collision facts quoted in docs

def test_query_stripping_merges_rules_it_does_not_merely_blur_them(by_key):
    """Never route evar194 through the profiler's hard-coded ^([^?#]*) helpers
    (gwam_canada_retirement_eda.py S4b/S4c). Path-only matching is not a weaker match, it is a
    WRONG one: four distinct rules become one bucket."""
    paths = {}
    for key, href in by_key.items():
        paths.setdefault(href.split("?")[0].split("#")[0], []).append(key)
    assert len(paths) == 10, "16 hrefs should collapse to 10 distinct paths"
    merged = sorted((sorted(v) for v in paths.values() if len(v) > 1), key=len, reverse=True)
    assert merged[0] == ["signin_join_en", "signin_join_fr",
                         "signin_member_en", "signin_member_fr"]
    assert sorted(merged[1:]) == [
        ["app_android_en", "app_android_fr"],
        ["app_apple_en", "app_apple_fr"],
        ["signup_join_en", "signup_join_fr"],
    ]


def test_sponsor_and_advisor_survive_path_stripping_only_on_a_trailing_slash(by_key):
    """A trailing-slash normalizer -- the most innocuous-looking URL cleanup there is -- would
    collapse these four into two. There is no such normalizer in the repo today; keep it that way."""
    for rid in ("signin_sponsor", "signin_advisor"):
        en = by_key[f"{rid}_en"].split("?")[0]
        fr = by_key[f"{rid}_fr"].split("?")[0]
        assert en != fr, f"{rid} en/fr paths already collide"
        assert fr == en + "/", f"{rid} differs by more than a trailing slash: {en!r} vs {fr!r}"


def test_a_100_char_cap_would_make_two_rules_the_same_value(by_key):
    """C13's max(char_length(evar194)) is the gate. Adobe props cap at 100; signin_member/en and
    signin_join/en are identical through character 101 and diverge at 102 ('member/' vs
    'enrolment/'). If the feed caps at 100 those two rules are unrecoverable -- no matching
    strategy helps, and that has to reach the SME rather than be papered over."""
    a, b = by_key["signin_member_en"], by_key["signin_join_en"]
    common = 0
    for x, y in zip(a, b):
        if x != y:
            break
        common += 1
    assert common == 101
    assert a[:100] == b[:100], "a 100-char cap makes these two rules one value"
    assert len({h[:100] for h in by_key.values()}) == 15, "expected exactly one merge at 100"
    assert len({h[:128] for h in by_key.values()}) == 16, "all 16 must separate by 128 chars"


def test_find_advisor_fr_already_exceeds_the_255_byte_evar_cap(by_key):
    """262 chars. If evar194 is 255-capped this rule can never match exactly -- which is the
    whole reason C14 reports m_token alongside m_exact rather than instead of it. Its token sits
    at the front of the URL, so token matching survives truncation."""
    href = by_key["find_advisor_fr"]
    assert len(href) == 262
    assert len(href) > 255, "no longer over the cap -- re-check whether m_exact is viable again"
    assert "solutionsepargne-parler-a-un-conseiller" in href[:255], (
        "the token must survive a 255-byte truncation or this rule is unmatchable")


def test_two_hrefs_are_strict_prefixes_of_others(by_key):
    """Why C14 reports m_prefix as a diagnostic rather than using it as the match: startswith
    over-claims on exactly these two pairs."""
    pairs = sorted((a, b) for a in by_key for b in by_key
                   if a != b and by_key[b].startswith(by_key[a]))
    assert pairs == [("app_apple_en", "app_apple_fr"),
                     ("signin_member_fr", "signin_join_fr")]


def test_sponsor_en_is_one_char_short_of_its_structural_twin(by_key):
    """doc-20 Q15. 143 vs its identically-shaped sibling's 144, ending mid-token at 'en-C'.
    That matches no Adobe limit (100 / 255), so it reads as a copy/paste artefact in the SME's
    spreadsheet rather than data truncation -- match by token, and ask."""
    assert len(by_key["signin_sponsor_en"]) == 143
    assert len(by_key["signin_advisor_en"]) == 144
    assert by_key["signin_sponsor_en"].endswith("ui_locales=en-C")
    assert by_key["signin_advisor_en"].endswith("ui_locales=en-CA")


def _like_any(value, patterns):
    """What `col LIKE '%host%'` means, in Python. Every pattern in SCOPE_LOGIN_HOST_EXCLUDE is
    %-wrapped on both sides, so LIKE reduces to a case-insensitive substring test."""
    v = value.lower()
    for p in patterns:
        assert p.startswith("%") and p.endswith("%"), f"unanchored pattern {p!r}"
        if p.strip("%").lower() in v:
            return True
    return False


def test_the_d8_login_exclusion_would_delete_five_of_the_eight_rules(rules):
    """doc-16 D14, and the reason it is a standing prohibition rather than a code comment.

    SCOPE_LOGIN_HOST_EXCLUDE is matched against the PAGE url (01_bronze_ingest.py), which is
    load-bearing: five of the eight rules have hrefs ON those hosts, because the whole point of
    those rules is a click TOWARD login from a public marketing page. D8 excludes login page
    views, not intent-to-log-in measured on the public site.

    So this test asserts the hazard is REAL -- every one of those hrefs does match the
    exclusion list -- which is what makes folding post_evar194 into any url expression a silent
    data-loss bug rather than a stylistic choice. Probe section C15 measures the same gap
    against live data (excl_by_page_url ~0% vs excl_by_href ~100%)."""
    by_rule = {}
    for r in rules:
        by_rule.setdefault(r["rule_id"], []).append(r["href"])

    caught = sorted(rid for rid, hrefs in by_rule.items()
                    if all(_like_any(h, SCOPE_LOGIN_HOST_EXCLUDE) for h in hrefs))
    assert caught == ["signin_advisor", "signin_join", "signin_member",
                      "signin_sponsor", "signup_join"], (
        "the set of rules destroyed by an href-side login filter changed -- re-read D14 before "
        "touching anything that builds a url expression")

    survivors = sorted(set(by_rule) - set(caught))
    assert survivors == ["app_android", "app_apple", "find_advisor"]

    # The pages these clicks fire ON are ordinary public marketing pages and are NOT excluded.
    # That asymmetry is the entire distinction D8 turns on.
    for page in ("https://www.manulife.com/ca/en/personal/group-plans/group-retirement.html",
                 "https://www.manulife.ca/ca/fr/particuliers/regimes-collectifs/retraite-collective"):
        assert not _like_any(page, SCOPE_LOGIN_HOST_EXCLUDE), (
            f"{page} is being excluded as a login host -- the rules would never fire")
