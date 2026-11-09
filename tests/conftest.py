"""
Test bootstrap: point the parameter loader at tests/test_config.yaml
(ILLUSTRATIVE values, not the tuned ones) BEFORE any paper_trading module
is imported, and make the repo root importable.
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ["PT_CONFIG"] = str(ROOT / "tests" / "test_config.yaml")
sys.path.insert(0, str(ROOT))
