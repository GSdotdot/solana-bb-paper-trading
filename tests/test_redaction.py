"""
Redaction guarantees of the public showcase.

1. config.example.yaml covers every key the code and tests use, so the
   placeholder file is a complete map of where tuned values apply.
2. Every leaf in config.example.yaml is a placeholder (XXXX) or an empty
   container — no real value can slip into the public template.
3. A redacted value loads as a sentinel that fails loudly when used.
"""
import os
from pathlib import Path

import yaml

from paper_trading.config import params

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "config.example.yaml"
TEST_CFG = ROOT / "tests" / "test_config.yaml"


def _load(p):
    with open(p, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _leaves(node, path=()):
    if isinstance(node, dict) and node:
        for k, v in node.items():
            yield from _leaves(v, path + (k,))
    else:
        yield path, node


def test_example_config_has_every_key_the_test_config_has():
    example = _load(EXAMPLE)
    for path, _ in _leaves(_load(TEST_CFG)):
        node = example
        for part in path:
            if node == params.PLACEHOLDER:       # whole subtree redacted
                break
            assert isinstance(node, dict) and part in node, f"missing {'.'.join(path)}"
            node = node[part]


def test_every_example_value_is_a_placeholder():
    for path, value in _leaves(_load(EXAMPLE)):
        assert value == params.PLACEHOLDER or value == {}, f"{'.'.join(path)} = {value!r}"


def test_redacted_values_fail_loudly():
    old = os.environ.get("PT_CONFIG")
    os.environ["PT_CONFIG"] = str(EXAMPLE)
    params.reload()
    try:
        v = params.param("friction.dex_fee_pct")
        assert isinstance(v, params.Redacted) and repr(v) == "XXXX"
        for use in (lambda: v * 2, lambda: v > 0, lambda: float(v)):
            try:
                use()
            except params.RedactedParameterError:
                continue
            raise AssertionError("redacted value was usable")
    finally:
        os.environ["PT_CONFIG"] = old
        params.reload()
