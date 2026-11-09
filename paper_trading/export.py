"""Shortcut: python -m paper_trading.export → routes to export command."""
import sys
sys.argv = [sys.argv[0], "export"] + sys.argv[1:]
from paper_trading.__main__ import main
