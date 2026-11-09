"""
Metrics Calculator (Layer 5)
============================
Computes all derived analytics from the database.

All methods are pure reads — they query the DB and return computed results.
No state is held between calls.

Metric categories:
  - Strategy-level: win rate, profit factor, avg PnL, Sharpe, drawdown
  - Cycle-level: duration distribution, exit type breakdown, partial vs full
  - Cost analysis: total fees, slippage impact, cost per cycle
  - Equity curve: time-series portfolio value for charting
  - Per-token: best/worst tokens, cycle counts by token
"""

from datetime import datetime
from typing import Dict, List, Optional
import math

from paper_trading.config.params import param
from paper_trading.recording.database import DatabaseManager

INITIAL_BALANCE = param("strategies.defaults.initial_balance")


class MetricsCalculator:
    """
    Read-only analytics engine. Queries the database and returns metrics.
    """

    def __init__(self, db: DatabaseManager):
        self.db = db

    # ════════════════════════════════════════════════════════════
    # STRATEGY SUMMARY
    # ════════════════════════════════════════════════════════════

    def strategy_summary(self, strategy_name: str) -> dict:
        """
        Comprehensive summary for one strategy instance.
        Returns dict with all key metrics.
        """
        cycles = self.db.get_cycles(strategy_name, limit=100_000)
        if not cycles:
            return self._empty_summary(strategy_name)

        pnls = [c["net_pnl"] for c in cycles]
        winners = [p for p in pnls if p > 0]
        losers = [p for p in pnls if p <= 0]
        durations = [c["duration_seconds"] for c in cycles]

        total_pnl = sum(pnls)
        gross_profit = sum(winners)
        gross_loss = abs(sum(losers))

        # Exit type breakdown
        exit_types: Dict[str, int] = {}
        for c in cycles:
            et = c["exit_type"]
            exit_types[et] = exit_types.get(et, 0) + 1

        # Partial exit rate (BBDOWN)
        partials = sum(1 for c in cycles if c["had_partial"])

        # Cost breakdown
        total_costs = sum(c["total_costs"] for c in cycles)

        # Consecutive wins/losses
        max_consec_win, max_consec_loss = self._consecutive_streaks(pnls)

        # Drawdown from cycle PnL series
        max_dd, max_dd_pct = self._max_drawdown_from_pnls(pnls, INITIAL_BALANCE)

        return {
            "strategy_name": strategy_name,
            "total_cycles": len(cycles),
            "total_pnl": round(total_pnl, 4),
            "total_pnl_pct": round(total_pnl / INITIAL_BALANCE * 100, 2),

            # Win/loss
            "winners": len(winners),
            "losers": len(losers),
            "win_rate": round(len(winners) / len(cycles) * 100, 1) if cycles else 0,
            "avg_win": round(gross_profit / len(winners), 4) if winners else 0,
            "avg_loss": round(-gross_loss / len(losers), 4) if losers else 0,
            "best_trade": round(max(pnls), 4),
            "worst_trade": round(min(pnls), 4),
            "median_pnl": round(sorted(pnls)[len(pnls) // 2], 4),

            # Ratios
            "profit_factor": round(gross_profit / gross_loss, 3) if gross_loss > 0 else float('inf'),
            "avg_pnl_per_cycle": round(total_pnl / len(cycles), 4),
            "expectancy": round(total_pnl / len(cycles), 4),

            # Risk
            "max_drawdown_usd": round(max_dd, 4),
            "max_drawdown_pct": round(max_dd_pct, 2),
            "max_consecutive_wins": max_consec_win,
            "max_consecutive_losses": max_consec_loss,

            # Duration
            "avg_duration_sec": round(sum(durations) / len(durations)) if durations else 0,
            "min_duration_sec": min(durations) if durations else 0,
            "max_duration_sec": max(durations) if durations else 0,

            # Exit types
            "exit_type_breakdown": exit_types,
            "partial_exit_rate": round(partials / len(cycles) * 100, 1) if cycles else 0,

            # Costs
            "total_costs": round(total_costs, 4),
            "avg_cost_per_cycle": round(total_costs / len(cycles), 4) if cycles else 0,
        }

    # ════════════════════════════════════════════════════════════
    # EQUITY CURVE
    # ════════════════════════════════════════════════════════════

    def equity_curve(self, strategy_name: str) -> List[dict]:
        """
        Build equity curve from portfolio snapshots.
        Returns list of {timestamp, total_value, cash, positions, drawdown_pct}.
        """
        snapshots = self.db.get_snapshots(strategy_name, limit=100_000)
        if not snapshots:
            return []

        peak = 0.0
        curve = []
        for s in snapshots:
            val = s["total_value"]
            if val > peak:
                peak = val
            dd_pct = ((peak - val) / peak * 100) if peak > 0 else 0.0
            curve.append({
                "timestamp": s["timestamp"],
                "total_value": val,
                "cash_balance": s["cash_balance"],
                "open_positions": s["open_positions"],
                "drawdown_pct": round(dd_pct, 2),
            })
        return curve

    def equity_curve_from_cycles(self, strategy_name: str,
                                  initial_balance: float = INITIAL_BALANCE) -> List[dict]:
        """
        Build equity curve from completed cycles (alternative to snapshots).
        Each point = cumulative PnL after each cycle close.
        """
        cycles = self.db.get_cycles(strategy_name, limit=100_000)
        if not cycles:
            return []

        balance = initial_balance
        peak = initial_balance
        curve = []
        for c in cycles:
            balance += c["net_pnl"]
            if balance > peak:
                peak = balance
            dd_pct = ((peak - balance) / peak * 100) if peak > 0 else 0.0
            curve.append({
                "timestamp": c["exit_timestamp"],
                "balance": round(balance, 4),
                "cycle_pnl": round(c["net_pnl"], 4),
                "cumulative_pnl": round(balance - initial_balance, 4),
                "drawdown_pct": round(dd_pct, 2),
            })
        return curve

    # ════════════════════════════════════════════════════════════
    # COST ANALYSIS
    # ════════════════════════════════════════════════════════════

    def cost_breakdown(self, strategy_name: str = None) -> dict:
        """Detailed cost analysis from raw trades."""
        trades = self.db.get_trades(strategy_name, limit=100_000)
        if not trades:
            return {"total_dex_fees": 0, "total_slippage": 0, "total_priority": 0,
                    "total_costs": 0, "trade_count": 0}

        dex = sum(t["dex_fee_usd"] for t in trades)
        slip = sum(t["slippage_usd"] for t in trades)
        prio = sum(t["priority_fee_usd"] for t in trades)

        return {
            "total_dex_fees": round(dex, 4),
            "total_slippage": round(slip, 4),
            "total_priority_fees": round(prio, 4),
            "total_costs": round(dex + slip + prio, 4),
            "trade_count": len(trades),
            "avg_cost_per_trade": round((dex + slip + prio) / len(trades), 4),
            "dex_fee_pct_of_total": round(dex / (dex + slip + prio) * 100, 1) if (dex + slip + prio) > 0 else 0,
            "slippage_pct_of_total": round(slip / (dex + slip + prio) * 100, 1) if (dex + slip + prio) > 0 else 0,
        }

    # ════════════════════════════════════════════════════════════
    # PER-TOKEN ANALYSIS
    # ════════════════════════════════════════════════════════════

    def per_token_summary(self, strategy_name: str) -> List[dict]:
        """PnL breakdown by token for a strategy."""
        cycles = self.db.get_cycles(strategy_name, limit=100_000)
        if not cycles:
            return []

        by_token: Dict[str, dict] = {}
        for c in cycles:
            ca = c["token_ca"]
            if ca not in by_token:
                by_token[ca] = {
                    "token_ca": ca,
                    "token_ticker": c["token_ticker"],
                    "cycles": 0, "wins": 0, "losses": 0,
                    "total_pnl": 0.0, "best": -float('inf'), "worst": float('inf'),
                }
            t = by_token[ca]
            t["cycles"] += 1
            t["total_pnl"] += c["net_pnl"]
            if c["net_pnl"] > 0:
                t["wins"] += 1
            else:
                t["losses"] += 1
            t["best"] = max(t["best"], c["net_pnl"])
            t["worst"] = min(t["worst"], c["net_pnl"])

        result = list(by_token.values())
        for r in result:
            r["total_pnl"] = round(r["total_pnl"], 4)
            r["best"] = round(r["best"], 4)
            r["worst"] = round(r["worst"], 4)
            r["win_rate"] = round(r["wins"] / r["cycles"] * 100, 1) if r["cycles"] else 0

        result.sort(key=lambda x: x["total_pnl"], reverse=True)
        return result

    # ════════════════════════════════════════════════════════════
    # CROSS-STRATEGY COMPARISON
    # ════════════════════════════════════════════════════════════

    def comparison_table(self, strategy_names: List[str]) -> List[dict]:
        """Side-by-side comparison of multiple strategies."""
        return [self.strategy_summary(name) for name in strategy_names]

    # ════════════════════════════════════════════════════════════
    # HELPERS
    # ════════════════════════════════════════════════════════════

    @staticmethod
    def _consecutive_streaks(pnls: List[float]) -> tuple:
        """Returns (max_consecutive_wins, max_consecutive_losses)."""
        max_win = max_loss = cur_win = cur_loss = 0
        for p in pnls:
            if p > 0:
                cur_win += 1
                cur_loss = 0
            else:
                cur_loss += 1
                cur_win = 0
            max_win = max(max_win, cur_win)
            max_loss = max(max_loss, cur_loss)
        return max_win, max_loss

    @staticmethod
    def _max_drawdown_from_pnls(pnls: List[float],
                                 initial: float) -> tuple:
        """Compute max drawdown from PnL series. Returns (usd, pct)."""
        balance = initial
        peak = initial
        max_dd = 0.0
        max_dd_pct = 0.0
        for p in pnls:
            balance += p
            if balance > peak:
                peak = balance
            dd = peak - balance
            dd_pct = (dd / peak * 100) if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd
                max_dd_pct = dd_pct
        return max_dd, max_dd_pct

    @staticmethod
    def _empty_summary(strategy_name: str) -> dict:
        return {
            "strategy_name": strategy_name,
            "total_cycles": 0, "total_pnl": 0, "total_pnl_pct": 0,
            "winners": 0, "losers": 0, "win_rate": 0,
            "avg_win": 0, "avg_loss": 0, "best_trade": 0, "worst_trade": 0,
            "median_pnl": 0, "profit_factor": 0, "avg_pnl_per_cycle": 0,
            "expectancy": 0, "max_drawdown_usd": 0, "max_drawdown_pct": 0,
            "max_consecutive_wins": 0, "max_consecutive_losses": 0,
            "avg_duration_sec": 0, "min_duration_sec": 0, "max_duration_sec": 0,
            "exit_type_breakdown": {}, "partial_exit_rate": 0,
            "total_costs": 0, "avg_cost_per_cycle": 0,
        }
