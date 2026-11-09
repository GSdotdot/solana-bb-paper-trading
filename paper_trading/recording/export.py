"""
Export Manager
===============
Generates Excel workbook and/or CSV files from paper trading SQLite database.

Sheets / files produced:
  1. Summary Dashboard   — per-strategy metrics side-by-side
  2. Trades              — every execution with full cost breakdown
  3. Cycles              — reconstructed entry+exits with PnL, exit type, duration
  4. Equity Curves       — timestamp, balance, drawdown per strategy
  5. Per-Token Summary   — performance by token across all strategies
  6. Cost Analysis       — fee breakdown (DEX, slippage, priority) per strategy
  7. Scoring Snapshots   — narrow_entry + full_promotion data for scoring analysis
  8. Entry Conditions    — score breakdown + strategy metrics at signal time
  9. OHLCV Archive       — raw candle data (CSV only — too large for Excel)

Usage:
    from paper_trading.recording.export import ExportManager
    em = ExportManager("paper_trading.db")
    em.export_excel("results.xlsx")       # Multi-sheet workbook
    em.export_csvs("exports/")            # Individual CSV files
    em.export_all("exports/")             # Both

CLI:
    python -m paper_trading.export --db paper_trading.db --output exports/
"""

import csv
import logging
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from paper_trading.config.params import param

INITIAL_BALANCE = param("strategies.defaults.initial_balance")

logger = logging.getLogger(__name__)


class ExportManager:
    """
    Reads from SQLite and writes Excel/CSV exports.
    No dependency on other paper_trading modules — works standalone
    with just the database file.
    """

    def __init__(self, db_path: str = "paper_trading.db"):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None

    def _open(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row

    def _close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def _query(self, sql: str, params=()) -> List[dict]:
        self._open()
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def _get_strategy_names(self) -> List[str]:
        rows = self._query(
            "SELECT DISTINCT strategy_name FROM cycles ORDER BY strategy_name"
        )
        return [r["strategy_name"] for r in rows]

    # ═══════════════════════════════════════════════════════════
    # DATA FETCHERS
    # ═══════════════════════════════════════════════════════════

    def _fetch_trades(self) -> List[dict]:
        return self._query("""
            SELECT trade_id, strategy_name, token_ca, token_ticker,
                   signal_type, timestamp,
                   fill_price, execution_price, candle_close, market_cap,
                   usd_amount, token_quantity,
                   slippage_pct, slippage_usd,
                   dex_fee_usd, priority_fee_usd, total_cost_usd, net_usd,
                   cash_after, positions_after,
                   ind_sma, ind_bb_upper, ind_bb_lower, ind_bb_width, ind_rsi, ind_vol_avg,
                   sell_pct, reason, cycle_id
            FROM trades ORDER BY timestamp
        """)

    def _fetch_cycles(self) -> List[dict]:
        return self._query("""
            SELECT cycle_id, strategy_name, token_ca, token_ticker,
                   entry_trade_id, entry_timestamp, entry_fill_price, entry_exec_price,
                   entry_market_cap, entry_tokens, entry_invested,
                   exit_type, exit_timestamp, exit_fill_price, exit_exec_price, exit_market_cap,
                   had_partial,
                   partial_trade_id, partial_timestamp, partial_fill_price, partial_exec_price,
                   partial_tokens, partial_net_usd,
                   total_proceeds, total_costs, net_pnl, pnl_pct,
                   duration_seconds, candles_in_trade,
                   mfe_price, mae_price
            FROM cycles ORDER BY exit_timestamp
        """)

    def _fetch_snapshots(self) -> List[dict]:
        return self._query(
            "SELECT * FROM portfolio_snapshots ORDER BY timestamp"
        )

    def _fetch_scoring_snapshots(self) -> List[dict]:
        return self._query(
            "SELECT * FROM scoring_snapshots ORDER BY timestamp"
        )

    def _fetch_entry_conditions(self) -> List[dict]:
        return self._query(
            "SELECT * FROM entry_conditions ORDER BY timestamp"
        )

    def _fetch_ohlcv(self, limit: int = 500_000) -> List[dict]:
        return self._query(
            "SELECT * FROM ohlcv_archive ORDER BY token_ca, timeframe, timestamp LIMIT ?",
            (limit,)
        )

    def _compute_strategy_summary(self, strategy_name: str,
                                   cycles: List[dict]) -> dict:
        """Compute summary metrics for one strategy from cycle data."""
        sc = [c for c in cycles if c["strategy_name"] == strategy_name]
        if not sc:
            return {"strategy_name": strategy_name, "total_cycles": 0}

        pnls = [c["net_pnl"] for c in sc]
        winners = [p for p in pnls if p > 0]
        losers = [p for p in pnls if p <= 0]
        gross_profit = sum(winners)
        gross_loss = abs(sum(losers))
        durations = [c["duration_seconds"] for c in sc]
        costs = sum(c["total_costs"] for c in sc)

        exit_types = {}
        for c in sc:
            et = c["exit_type"]
            exit_types[et] = exit_types.get(et, 0) + 1

        balance = INITIAL_BALANCE
        peak = INITIAL_BALANCE
        max_dd = 0.0
        for p in pnls:
            balance += p
            peak = max(peak, balance)
            max_dd = max(max_dd, peak - balance)

        return {
            "strategy_name": strategy_name,
            "total_cycles": len(sc),
            "winners": len(winners),
            "losers": len(losers),
            "win_rate_pct": round(len(winners) / len(sc) * 100, 1),
            "total_pnl": round(sum(pnls), 4),
            "avg_pnl": round(sum(pnls) / len(sc), 4),
            "best_trade": round(max(pnls), 4),
            "worst_trade": round(min(pnls), 4),
            "profit_factor": round(gross_profit / gross_loss, 3) if gross_loss > 0 else 0,
            "avg_win": round(gross_profit / len(winners), 4) if winners else 0,
            "avg_loss": round(-gross_loss / len(losers), 4) if losers else 0,
            "max_drawdown": round(max_dd, 4),
            "avg_duration_min": round(sum(durations) / len(durations) / 60, 1),
            "total_costs": round(costs, 4),
            "exit_types": str(exit_types),
        }

    def _compute_per_token(self, cycles: List[dict]) -> List[dict]:
        """Aggregate cycle data by token across all strategies."""
        by_token: Dict[str, dict] = {}
        for c in cycles:
            key = c["token_ca"]
            if key not in by_token:
                by_token[key] = {
                    "token_ca": key,
                    "token_ticker": c["token_ticker"],
                    "strategies": set(),
                    "cycles": 0, "wins": 0, "total_pnl": 0.0,
                    "best": -1e18, "worst": 1e18,
                }
            t = by_token[key]
            t["strategies"].add(c["strategy_name"])
            t["cycles"] += 1
            t["total_pnl"] += c["net_pnl"]
            if c["net_pnl"] > 0:
                t["wins"] += 1
            t["best"] = max(t["best"], c["net_pnl"])
            t["worst"] = min(t["worst"], c["net_pnl"])

        result = []
        for t in by_token.values():
            result.append({
                "token_ca": t["token_ca"],
                "token_ticker": t["token_ticker"],
                "strategies": ", ".join(sorted(t["strategies"])),
                "cycles": t["cycles"],
                "wins": t["wins"],
                "win_rate_pct": round(t["wins"] / t["cycles"] * 100, 1),
                "total_pnl": round(t["total_pnl"], 4),
                "best_trade": round(t["best"], 4),
                "worst_trade": round(t["worst"], 4),
            })
        result.sort(key=lambda x: x["total_pnl"], reverse=True)
        return result

    def _compute_equity_curves(self, cycles: List[dict]) -> Dict[str, List[dict]]:
        """Build equity curve per strategy from chronological cycles."""
        curves: Dict[str, List[dict]] = {}
        for name in sorted(set(c["strategy_name"] for c in cycles)):
            sc = [c for c in cycles if c["strategy_name"] == name]
            sc.sort(key=lambda c: c["exit_timestamp"])
            balance = INITIAL_BALANCE
            peak = INITIAL_BALANCE
            curve = []
            for c in sc:
                balance += c["net_pnl"]
                peak = max(peak, balance)
                dd_pct = ((peak - balance) / peak * 100) if peak > 0 else 0
                curve.append({
                    "strategy": name,
                    "timestamp": c["exit_timestamp"],
                    "balance": round(balance, 4),
                    "cycle_pnl": round(c["net_pnl"], 4),
                    "cumulative_pnl": round(balance - INITIAL_BALANCE, 4),
                    "drawdown_pct": round(dd_pct, 2),
                })
            curves[name] = curve
        return curves

    def _compute_cost_analysis(self, trades: List[dict]) -> List[dict]:
        """Cost breakdown per strategy."""
        by_strat: Dict[str, dict] = {}
        for t in trades:
            name = t["strategy_name"]
            if name not in by_strat:
                by_strat[name] = {"strategy": name, "trades": 0,
                                   "dex_fees": 0, "slippage": 0, "priority_fees": 0}
            s = by_strat[name]
            s["trades"] += 1
            s["dex_fees"] += t["dex_fee_usd"]
            s["slippage"] += t["slippage_usd"]
            s["priority_fees"] += t["priority_fee_usd"]

        result = []
        for s in by_strat.values():
            total = s["dex_fees"] + s["slippage"] + s["priority_fees"]
            result.append({
                "strategy": s["strategy"],
                "trade_count": s["trades"],
                "dex_fees": round(s["dex_fees"], 4),
                "slippage": round(s["slippage"], 4),
                "priority_fees": round(s["priority_fees"], 4),
                "total_costs": round(total, 4),
                "avg_cost_per_trade": round(total / s["trades"], 4) if s["trades"] else 0,
            })
        result.sort(key=lambda x: x["strategy"])
        return result

    # ═══════════════════════════════════════════════════════════
    # CSV EXPORT
    # ═══════════════════════════════════════════════════════════

    def export_csvs(self, output_dir: str = "exports"):
        """Export all data as individual CSV files."""
        os.makedirs(output_dir, exist_ok=True)
        self._open()

        trades = self._fetch_trades()
        cycles = self._fetch_cycles()
        strat_names = sorted(set(c["strategy_name"] for c in cycles)) if cycles else []

        # 1. Trades
        if trades:
            self._write_csv(f"{output_dir}/trades.csv", trades)
            logger.info(f"Exported {len(trades)} trades")

        # 2. Cycles
        if cycles:
            self._write_csv(f"{output_dir}/cycles.csv", cycles)
            logger.info(f"Exported {len(cycles)} cycles")

        # 3. Strategy summaries
        if strat_names:
            summaries = [self._compute_strategy_summary(n, cycles) for n in strat_names]
            self._write_csv(f"{output_dir}/strategy_summary.csv", summaries)
            logger.info(f"Exported {len(summaries)} strategy summaries")

        # 4. Equity curves (all strategies in one file)
        if cycles:
            curve_data = self._compute_equity_curves(cycles)
            all_points = []
            for pts in curve_data.values():
                all_points.extend(pts)
            if all_points:
                self._write_csv(f"{output_dir}/equity_curves.csv", all_points)
                logger.info(f"Exported {len(all_points)} equity curve points")

        # 5. Per-token summary
        if cycles:
            per_token = self._compute_per_token(cycles)
            self._write_csv(f"{output_dir}/per_token_summary.csv", per_token)
            logger.info(f"Exported {len(per_token)} token summaries")

        # 6. Cost analysis
        if trades:
            costs = self._compute_cost_analysis(trades)
            self._write_csv(f"{output_dir}/cost_analysis.csv", costs)

        # 7. Scoring snapshots
        try:
            scoring = self._fetch_scoring_snapshots()
            if scoring:
                self._write_csv(f"{output_dir}/scoring_snapshots.csv", scoring)
                logger.info(f"Exported {len(scoring)} scoring snapshots")
        except sqlite3.OperationalError:
            pass  # Table may not exist if scoring wasn't enabled

        # 8. Entry conditions
        try:
            entries = self._fetch_entry_conditions()
            if entries:
                self._write_csv(f"{output_dir}/entry_conditions.csv", entries)
                logger.info(f"Exported {len(entries)} entry conditions")
        except sqlite3.OperationalError:
            pass

        # 9. OHLCV archive (can be huge)
        try:
            ohlcv = self._fetch_ohlcv()
            if ohlcv:
                self._write_csv(f"{output_dir}/ohlcv_archive.csv", ohlcv)
                logger.info(f"Exported {len(ohlcv)} OHLCV candles")
        except sqlite3.OperationalError:
            pass

        self._close()
        logger.info(f"CSV export complete → {output_dir}/")

    @staticmethod
    def _write_csv(filepath: str, rows: List[dict]):
        if not rows:
            return
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    # ═══════════════════════════════════════════════════════════
    # EXCEL EXPORT
    # ═══════════════════════════════════════════════════════════

    def export_excel(self, filepath: str = "paper_trading_results.xlsx"):
        """
        Export multi-sheet Excel workbook with formatting.
        Sheets: Summary, Trades, Cycles, Equity Curves, Per-Token, Costs,
                Scoring Snapshots, Entry Conditions.
        (OHLCV archive excluded — too large for Excel, use CSV.)
        """
        try:
            from openpyxl import Workbook
            from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers
            from openpyxl.utils import get_column_letter
        except ImportError:
            raise ImportError("openpyxl required: pip install openpyxl")

        self._open()
        wb = Workbook()

        # Style definitions
        header_font = Font(name="Arial", bold=True, color="FFFFFF", size=10)
        header_fill = PatternFill("solid", fgColor="2F5496")
        data_font = Font(name="Arial", size=10)
        money_fmt = '#,##0.0000;(#,##0.0000);"-"'
        pct_fmt = '0.0%'
        int_fmt = '#,##0'
        border = Border(
            bottom=Side(style="thin", color="D9D9D9")
        )

        def write_sheet(ws, title, rows, col_formats=None):
            """Write data rows to a sheet with formatting."""
            if not rows:
                ws.append(["No data"])
                return

            headers = list(rows[0].keys())
            ws.append(headers)
            for i, h in enumerate(headers, 1):
                cell = ws.cell(row=1, column=i)
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = Alignment(horizontal="center", wrap_text=True)

            for row_data in rows:
                ws.append(list(row_data.values()))

            # Auto-width
            for i, h in enumerate(headers, 1):
                max_len = len(str(h))
                for row in ws.iter_rows(min_row=2, min_col=i, max_col=i):
                    for cell in row:
                        cell.font = data_font
                        cell.border = border
                        val_len = len(str(cell.value or ""))
                        if val_len > max_len:
                            max_len = val_len
                ws.column_dimensions[get_column_letter(i)].width = min(max_len + 3, 30)

            # Apply column formats
            if col_formats:
                for col_name, fmt in col_formats.items():
                    if col_name in headers:
                        col_idx = headers.index(col_name) + 1
                        for row in ws.iter_rows(min_row=2, min_col=col_idx, max_col=col_idx):
                            for cell in row:
                                cell.number_format = fmt

            ws.freeze_panes = "A2"
            ws.auto_filter.ref = ws.dimensions

        # ── Fetch all data ───────────────────────────────────
        trades = self._fetch_trades()
        cycles = self._fetch_cycles()
        strat_names = sorted(set(c["strategy_name"] for c in cycles)) if cycles else []

        # ── Sheet 1: Summary Dashboard ───────────────────────
        ws = wb.active
        ws.title = "Summary"
        summaries = [self._compute_strategy_summary(n, cycles) for n in strat_names]
        write_sheet(ws, "Summary", summaries, {
            "win_rate_pct": '0.0',
            "total_pnl": money_fmt, "avg_pnl": money_fmt,
            "best_trade": money_fmt, "worst_trade": money_fmt,
            "profit_factor": '0.000',
            "avg_win": money_fmt, "avg_loss": money_fmt,
            "max_drawdown": money_fmt, "total_costs": money_fmt,
        })

        # ── Sheet 2: All Trades ──────────────────────────────
        ws_trades = wb.create_sheet("Trades")
        write_sheet(ws_trades, "Trades", trades, {
            "fill_price": '#,##0.00000000', "execution_price": '#,##0.00000000',
            "usd_amount": money_fmt, "net_usd": money_fmt, "total_cost_usd": money_fmt,
            "dex_fee_usd": money_fmt, "slippage_usd": money_fmt, "priority_fee_usd": money_fmt,
        })

        # ── Sheet 3: Cycles ──────────────────────────────────
        ws_cycles = wb.create_sheet("Cycles")
        write_sheet(ws_cycles, "Cycles", cycles, {
            "entry_fill_price": '#,##0.00000000', "exit_fill_price": '#,##0.00000000',
            "entry_exec_price": '#,##0.00000000', "exit_exec_price": '#,##0.00000000',
            "net_pnl": money_fmt, "total_costs": money_fmt,
            "total_proceeds": money_fmt, "entry_invested": money_fmt,
            "pnl_pct": '0.00',
        })

        # ── Sheet 4: Equity Curves ───────────────────────────
        ws_eq = wb.create_sheet("Equity Curves")
        if cycles:
            curve_data = self._compute_equity_curves(cycles)
            all_points = []
            for pts in curve_data.values():
                all_points.extend(pts)
            write_sheet(ws_eq, "Equity Curves", all_points, {
                "balance": money_fmt, "cycle_pnl": money_fmt,
                "cumulative_pnl": money_fmt, "drawdown_pct": '0.00',
            })

        # ── Sheet 5: Per-Token Summary ───────────────────────
        ws_token = wb.create_sheet("Per-Token")
        if cycles:
            per_token = self._compute_per_token(cycles)
            write_sheet(ws_token, "Per-Token", per_token, {
                "win_rate_pct": '0.0', "total_pnl": money_fmt,
                "best_trade": money_fmt, "worst_trade": money_fmt,
            })

        # ── Sheet 6: Cost Analysis ───────────────────────────
        ws_cost = wb.create_sheet("Cost Analysis")
        if trades:
            costs = self._compute_cost_analysis(trades)
            write_sheet(ws_cost, "Cost Analysis", costs, {
                "dex_fees": money_fmt, "slippage": money_fmt,
                "priority_fees": money_fmt, "total_costs": money_fmt,
                "avg_cost_per_trade": money_fmt,
            })

        # ── Sheet 7: Scoring Snapshots ───────────────────────
        ws_scoring = wb.create_sheet("Scoring Snapshots")
        try:
            scoring = self._fetch_scoring_snapshots()
            # Exclude the large overview_json column from Excel
            scoring_clean = []
            for row in scoring:
                r = dict(row)
                r.pop("overview_json", None)
                scoring_clean.append(r)
            write_sheet(ws_scoring, "Scoring Snapshots", scoring_clean, {
                "market_cap": '#,##0', "liquidity": '#,##0',
                "volume_24h_usd": '#,##0', "progress_percent": '0.00',
                "top10_holder_pct": '0.0000', "top10_user_pct": '0.0000',
            })
        except sqlite3.OperationalError:
            ws_scoring.append(["Scoring data not available"])

        # ── Sheet 8: Entry Conditions ────────────────────────
        ws_entry = wb.create_sheet("Entry Conditions")
        try:
            entries = self._fetch_entry_conditions()
            write_sheet(ws_entry, "Entry Conditions", entries, {
                "volume_per_candle": '#,##0.00', "bb_width_pct": '0.00',
                "vol_mc_ratio": '0.00', "market_cap": '#,##0',
            })
        except sqlite3.OperationalError:
            ws_entry.append(["Entry conditions data not available"])

        # ── Save ─────────────────────────────────────────────
        wb.save(filepath)
        self._close()
        logger.info(f"Excel export complete → {filepath}")
        logger.info(f"  Strategies: {len(strat_names)}")
        logger.info(f"  Trades: {len(trades)}")
        logger.info(f"  Cycles: {len(cycles)}")

    # ═══════════════════════════════════════════════════════════
    # COMBINED EXPORT
    # ═══════════════════════════════════════════════════════════

    def export_all(self, output_dir: str = "exports"):
        """Export both Excel workbook and individual CSVs."""
        os.makedirs(output_dir, exist_ok=True)
        self.export_excel(f"{output_dir}/paper_trading_results.xlsx")
        self.export_csvs(output_dir)


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

def main():
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Export paper trading results")
    parser.add_argument("--db", type=str, default="paper_trading.db",
                        help="Path to SQLite database")
    parser.add_argument("--output", type=str, default="exports",
                        help="Output directory")
    parser.add_argument("--format", type=str, default="all",
                        choices=["excel", "csv", "all"],
                        help="Export format")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if not os.path.exists(args.db):
        print(f"Database not found: {args.db}")
        sys.exit(1)

    em = ExportManager(args.db)

    if args.format == "excel":
        os.makedirs(args.output, exist_ok=True)
        em.export_excel(f"{args.output}/paper_trading_results.xlsx")
    elif args.format == "csv":
        em.export_csvs(args.output)
    else:
        em.export_all(args.output)


if __name__ == "__main__":
    main()
