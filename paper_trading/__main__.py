"""
Paper Trading System — CLI Entry Point (Dual Funnel)
=====================================================
Usage: python -m paper_trading <command> [options]

Commands:
    run       Start the live paper trading system
    export    Export results to Excel/CSV from the database
    status    Quick status check on an existing database
    index     Show all available commands and options
"""

import argparse
import logging
import os
import sys


# ═══════════════════════════════════════════════════════════════
# COMMANDS
# ═══════════════════════════════════════════════════════════════

def cmd_run(args):
    """Start the live paper trading system."""
    import asyncio
    from paper_trading.main import PaperTradingSystem, setup_logging
    from paper_trading.orchestrator.config import SystemConfig

    api_key = args.api_key or os.environ.get("DATA_PROVIDER_API_KEY", "")
    if not api_key:
        print("ERROR: data-provider API key required.")
        print("  Use --api-key YOUR_KEY or set DATA_PROVIDER_API_KEY env var")
        sys.exit(1)

    setup_logging(args.log_level, args.log_file)

    config = SystemConfig(
        data_api_key=api_key,
        db_path=args.db,
        log_level=args.log_level,
        log_file=args.log_file,
    )

    system = PaperTradingSystem(config=config)
    try:
        asyncio.run(system.run())
    except KeyboardInterrupt:
        pass


def cmd_export(args):
    """Export results to Excel and/or CSV."""
    from paper_trading.recording.export import ExportManager

    if not os.path.exists(args.db):
        print(f"Database not found: {args.db}")
        sys.exit(1)

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    em = ExportManager(args.db)
    os.makedirs(args.output, exist_ok=True)

    if args.format == "excel":
        em.export_excel(f"{args.output}/paper_trading_results.xlsx")
    elif args.format == "csv":
        em.export_csvs(args.output)
    else:
        em.export_all(args.output)

    print(f"\nDone. Files in: {args.output}/")


def cmd_status(args):
    """Quick status check on an existing database."""
    import sqlite3

    if not os.path.exists(args.db):
        print(f"Database not found: {args.db}")
        sys.exit(1)

    conn = sqlite3.connect(args.db)

    def count(table):
        try:
            return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except sqlite3.OperationalError:
            return "—"

    def distinct(table, col):
        try:
            return conn.execute(
                f"SELECT COUNT(DISTINCT {col}) FROM {table}"
            ).fetchone()[0]
        except sqlite3.OperationalError:
            return "—"

    def latest(table, col="timestamp"):
        try:
            row = conn.execute(
                f"SELECT {col} FROM {table} ORDER BY {col} DESC LIMIT 1"
            ).fetchone()
            return row[0] if row else "—"
        except sqlite3.OperationalError:
            return "—"

    size_kb = os.path.getsize(args.db) / 1024
    print(f"""
{'='*55}
  PAPER TRADING DATABASE STATUS
  {args.db} ({size_kb:.0f} KB)
{'='*55}

  TRADING DATA
  ├─ Trades:              {count('trades'):>8}
  ├─ Cycles:              {count('cycles'):>8}
  ├─ Snapshots:           {count('portfolio_snapshots'):>8}
  ├─ Events:              {count('events'):>8}
  └─ Strategies active:   {distinct('cycles', 'strategy_name'):>8}

  SCORING DATA
  ├─ Scoring snapshots:   {count('scoring_snapshots'):>8}
  ├─ Entry conditions:    {count('entry_conditions'):>8}
  ├─ OHLCV candles:       {count('ohlcv_archive'):>8}
  └─ Unique tokens:       {distinct('scoring_snapshots', 'token_ca'):>8}

  TIMELINE
  ├─ Latest trade:        {latest('trades')}
  ├─ Latest cycle:        {latest('cycles', 'exit_timestamp')}
  └─ Latest candle:       {latest('ohlcv_archive')}
{'='*55}
""")
    conn.close()


def cmd_index(_args=None):
    """Show all available commands and their options."""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║           PAPER TRADING SYSTEM — DUAL FUNNEL — INDEX            ║
╚══════════════════════════════════════════════════════════════════╝

  SETUP
  ─────────────────────────────────────────────────────────────────
  pip install -r requirements.txt

  COMMANDS
  ─────────────────────────────────────────────────────────────────

  ┌─ RUN (start live paper trading)
  │
  │  python -m paper_trading run --api-key YOUR_KEY
  │  python -m paper_trading run --api-key YOUR_KEY --db my.db
  │
  │  Options:
  │    --api-key KEY        Data-provider API key (or DATA_PROVIDER_API_KEY env var)
  │    --db PATH            SQLite database path     [default: paper_trading.db]
  │    --log-level LEVEL    DEBUG|INFO|WARNING|ERROR  [default: INFO]
  │    --log-file PATH      Log file path             [default: paper_trading.log]
  │
  ├─ EXPORT (generate Excel + CSV from database)
  │
  │  python -m paper_trading export
  │  python -m paper_trading export --db my.db --output results/
  │
  ├─ STATUS (quick database check)
  │
  │  python -m paper_trading status
  │
  └─ INDEX (this help)
     python -m paper_trading index

  ARCHITECTURE (Dual Funnel — values redacted, see config.example.yaml)
  ─────────────────────────────────────────────────────────────────

  BBUP FUNNEL (own candle timeframe, phase-driven polling, volume pre-filter)
  ┌─ token list (min_volume_5m=XXXX) → Wide → Narrow → Full (XXXX slots)
  │                                                          ↓
  │   BBUP ablation instances (SMA=XXXX, BB=XXXX)
  │   Environment filter: avg volume within [XXXX, XXXX)
  │   Max XXXX concurrent positions per instance
  └─ WebSocket OHLCV on the BBUP timeframe

  BBDOWN FUNNEL (own candle timeframe, phase-driven polling, broad query)
  ┌─ token list (broad query) → Wide → Narrow → Full (XXXX slots)
  │                                                      ↓
  │   BBDOWN ablation instances (SMA=XXXX, BB=XXXX)
  │   Optional environment filter: band width within [XXXX, XXXX)
  │   Max XXXX concurrent positions per instance
  └─ WebSocket OHLCV on the BBDOWN timeframe

  FREEZE FIX
  ─────────────────────────────────────────────────────────────────
  - All DB writes run via thread executor (never block event loop)
  - Periodic heartbeat with task health monitoring + systemd watchdog ping
  - Dead task detection with CRITICAL-level logging
""")


# ═══════════════════════════════════════════════════════════════
# MAIN ROUTER
# ═══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        prog="python -m paper_trading",
        description="Paper Trading System — Dual Funnel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Run 'python -m paper_trading index' to see all commands.",
    )
    subparsers = parser.add_subparsers(dest="command")

    # run
    p_run = subparsers.add_parser("run", help="Start live paper trading")
    p_run.add_argument("--api-key", type=str, default="")
    p_run.add_argument("--db", type=str, default="paper_trading.db")
    p_run.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p_run.add_argument("--log-file", type=str, default="paper_trading.log")

    # export
    p_export = subparsers.add_parser("export", help="Export results to Excel/CSV")
    p_export.add_argument("--db", type=str, default="paper_trading.db")
    p_export.add_argument("--output", type=str, default="exports")
    p_export.add_argument("--format", type=str, default="all",
                           choices=["excel", "csv", "all"])

    # status
    p_status = subparsers.add_parser("status", help="Database status check")
    p_status.add_argument("--db", type=str, default="paper_trading.db")

    # index
    subparsers.add_parser("index", help="Show all commands and options")

    args = parser.parse_args()

    commands = {
        "run": cmd_run,
        "export": cmd_export,
        "status": cmd_status,
        "index": cmd_index,
    }

    handler = commands.get(args.command)
    if handler:
        handler(args)
    else:
        cmd_index()


main()
