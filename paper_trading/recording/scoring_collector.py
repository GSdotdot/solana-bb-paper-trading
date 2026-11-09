"""
Scoring Data Collector (Passive)
=================================
Records comprehensive data for post-run scoring analysis.
Makes ZERO trading decisions — purely observational.

Three collection points:
  1. Token Snapshots — at Narrow entry (from token-list, free) and
     Full promotion (token overview, one paid call). The delta between snapshots
     is itself a scoring signal.
  2. Entry Conditions — at the exact moment a strategy fires a signal.
     Score is computed but NOT acted on during paper trading.
  3. OHLCV Archive — every finalized candle for Full Focus tokens,
     streamed from WebSocket at zero extra cost.

Post-run workflow:
  JOIN scoring_snapshots ON cycles (token_ca, entry time window)
  → Correlate snapshot fields with trade outcomes
  → Identify which metrics predict winners vs losers
  → Build scoring weights from live data, not backtests

SQLite tables:
  - scoring_snapshots:  token state at Narrow entry + Full promotion
  - entry_conditions:   strategy-specific metrics at signal time
  - ohlcv_archive:      every candle for every Full Focus token
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional

from paper_trading.config.params import param
from paper_trading.core.models import Candle

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# SCORING CALCULATOR (passive — compute but don't act)
# ═══════════════════════════════════════════════════════════════

class ScoringCalculator:
    """
    Compute the multi-factor score derived from backtest analysis.

    This is logged alongside every trade but NOT used for
    entry/exit decisions during paper trading.

    Factors (tier thresholds and point weights tuned privately → config.scoring):
      Volume/candle:   tiered points           [XXXX]
      BB Width:        tiered points           [XXXX]
      Vol/MC Ratio:    tiered points           [XXXX]
      MC Sweet Spot:   bonus inside [XXXX, XXXX]
      Risk zone:       penalty when MC < XXXX AND BBW >= XXXX (BBDOWN only)
    """

    @staticmethod
    def _tier(value: float, tiers) -> int:
        """tiers: list of [min_value, points], highest threshold first."""
        for threshold, points in tiers:
            if value >= threshold:
                return points
        return 0

    @staticmethod
    def compute(
        volume_per_candle: float,
        bb_width_pct: float,
        market_cap: float,
        strategy_name: str = "",
    ) -> dict:
        """
        Compute score and return breakdown.
        Returns dict with total score + individual factor scores.
        """
        vol_mc_ratio = (volume_per_candle / market_cap * 100) if market_cap > 0 else 0

        S = param("scoring")
        tier = ScoringCalculator._tier

        vol_score = tier(volume_per_candle, S["volume_per_candle_tiers"])
        bbw_score = tier(bb_width_pct, S["bb_width_tiers"])
        vmc_score = tier(vol_mc_ratio, S["vol_mc_ratio_tiers"])

        # MC sweet spot
        mc_lo, mc_hi = S["mc_sweet_spot"]
        mc_score = S["mc_sweet_spot_points"] if mc_lo <= market_cap <= mc_hi else 0

        # Risk-zone penalty (BBDOWN only)
        risk_penalty = 0
        is_1h_risk = False
        rz = S["risk_zone"]
        if "bbdown" in strategy_name.lower():
            if market_cap < rz["max_mc"] and bb_width_pct >= rz["min_bb_width"]:
                risk_penalty = rz["penalty"]
                is_1h_risk = True

        total = vol_score + bbw_score + vmc_score + mc_score + risk_penalty

        return {
            "total_score": total,
            "volume_score": vol_score,
            "bb_width_score": bbw_score,
            "vol_mc_ratio_score": vmc_score,
            "mc_sweet_spot_score": mc_score,
            "risk_penalty": risk_penalty,
            "is_1h_risk_zone": is_1h_risk,
            # Raw values for analysis
            "volume_per_candle": round(volume_per_candle, 2),
            "bb_width_pct": round(bb_width_pct, 2),
            "vol_mc_ratio": round(vol_mc_ratio, 2),
            "market_cap": round(market_cap, 2),
        }


# ═══════════════════════════════════════════════════════════════
# DATA COLLECTOR
# ═══════════════════════════════════════════════════════════════

class ScoringDataCollector:
    """
    Passive data collection for post-run scoring analysis.

    Usage:
        collector = ScoringDataCollector("paper_trading.db")
        collector.open()

        # At Narrow entry (free — data from token-list):
        collector.log_snapshot(token, stage="narrow_entry")

        # At Full promotion (paid call — token overview data):
        collector.log_snapshot(token, stage="full_promotion", overview_data={...})

        # At trade entry (free — computed from strategy state):
        collector.log_entry_conditions(strategy, token_ca, signal, score)

        # On every candle (free — from WebSocket):
        collector.log_candle(candle)

        collector.close()
    """

    def __init__(self, db_path: str = "paper_trading.db"):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self._candle_buffer: List[tuple] = []
        self._buffer_limit = 100  # Flush every N candles

    def open(self):
        """Open connection and create scoring tables."""
        self.conn = sqlite3.connect(
            self.db_path, check_same_thread=False, timeout=30.0
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self._create_tables()
        logger.info("Scoring data collector opened")

    def close(self):
        """Flush buffers and close."""
        self._flush_candle_buffer()
        if self.conn:
            self.conn.close()
            self.conn = None

    def _create_tables(self):
        """Create scoring-specific tables (alongside main trading tables)."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS scoring_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_ca TEXT NOT NULL,
                ticker TEXT,
                stage TEXT NOT NULL,           -- 'narrow_entry' or 'full_promotion'
                timestamp TEXT NOT NULL,

                -- Market data
                price REAL,
                market_cap REAL,
                liquidity REAL,
                volume_5m_usd REAL,
                volume_1h_usd REAL,
                volume_24h_usd REAL,
                holder_count INTEGER,
                unique_wallet_24h INTEGER,
                trade_1h_count INTEGER,
                trade_24h_count INTEGER,

                -- Price momentum
                price_change_1h_pct REAL,
                price_change_24h_pct REAL,

                -- Volume momentum
                volume_1h_change_pct REAL,
                volume_5m_change_pct REAL,

                -- Buy/sell pressure
                buy_24h INTEGER,
                sell_24h INTEGER,
                volume_buy_24h_usd REAL,
                volume_sell_24h_usd REAL,

                -- Launchpad info
                launchpad_source TEXT,
                graduated INTEGER,             -- 0/1
                progress_percent REAL,
                launchpad_creator TEXT,
                real_sol_reserves REAL,
                platform_id TEXT,

                -- Security data (populated at full_promotion if available)
                top10_holder_pct REAL,
                top10_user_pct REAL,
                creator_pct REAL,
                freeze_authority TEXT,
                transfer_fee_enabled INTEGER,
                pre_market_holder_count INTEGER,

                -- Token overview data (only at full_promotion, paid call)
                overview_json TEXT,            -- Full overview response for future mining

                -- Funnel metadata
                list_appearances INTEGER,
                time_in_narrow_seconds REAL,
                liquidity_ratio REAL
            );

            CREATE INDEX IF NOT EXISTS idx_snapshots_token
                ON scoring_snapshots(token_ca, stage);

            CREATE TABLE IF NOT EXISTS entry_conditions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_ca TEXT NOT NULL,
                ticker TEXT,
                strategy_name TEXT NOT NULL,
                timestamp TEXT NOT NULL,

                -- Score breakdown
                total_score INTEGER,
                volume_score INTEGER,
                bb_width_score INTEGER,
                vol_mc_ratio_score INTEGER,
                mc_sweet_spot_score INTEGER,
                risk_penalty INTEGER,
                is_1h_risk_zone INTEGER,

                -- Raw values
                volume_per_candle REAL,
                bb_width_pct REAL,
                vol_mc_ratio REAL,
                market_cap REAL,

                -- Strategy-specific entry metrics
                rsi REAL,
                sma_value REAL,
                upper_band REAL,
                lower_band REAL,
                price_at_entry REAL,
                distance_from_lbb_pct REAL,
                distance_from_ubb_pct REAL,

                -- Context
                utc_hour INTEGER,
                candles_since_full_entry INTEGER,
                funnel_wide_fill REAL,
                funnel_narrow_fill REAL,
                funnel_full_fill REAL
            );

            CREATE INDEX IF NOT EXISTS idx_entry_token
                ON entry_conditions(token_ca, strategy_name);

            CREATE TABLE IF NOT EXISTS ohlcv_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token_ca TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                market_cap REAL
            );

            CREATE INDEX IF NOT EXISTS idx_ohlcv_token_time
                ON ohlcv_archive(token_ca, timeframe, timestamp);
        """)
        self.conn.commit()

    # ── Snapshot Logging ─────────────────────────────────────

    def log_snapshot(self, token, stage: str,
                     overview_data: Optional[dict] = None):
        """
        Record token state at a funnel transition point.

        Args:
            token: ScannerToken instance
            stage: "narrow_entry" or "full_promotion"
            overview_data: raw token_overview response (only at full_promotion)
        """
        if self.conn is None:
            return

        now = datetime.now(timezone.utc).isoformat()

        self.conn.execute("""
            INSERT INTO scoring_snapshots (
                token_ca, ticker, stage, timestamp,
                price, market_cap, liquidity,
                volume_5m_usd, volume_1h_usd, volume_24h_usd,
                holder_count, unique_wallet_24h, trade_1h_count, trade_24h_count,
                price_change_1h_pct, price_change_24h_pct,
                volume_1h_change_pct, volume_5m_change_pct,
                buy_24h, sell_24h, volume_buy_24h_usd, volume_sell_24h_usd,
                launchpad_source, graduated, progress_percent, launchpad_creator,
                real_sol_reserves, platform_id,
                top10_holder_pct, top10_user_pct, creator_pct,
                freeze_authority, transfer_fee_enabled, pre_market_holder_count,
                overview_json,
                list_appearances, time_in_narrow_seconds, liquidity_ratio
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?,
                ?, ?, ?
            )
        """, (
            token.token_ca, token.ticker, stage, now,
            token.price, token.market_cap, token.liquidity,
            token.volume_5m_usd, token.volume_1h_usd, token.volume_24h_usd,
            token.holder_count, token.unique_wallet_24h,
            token.trade_1h_count, token.trade_24h_count,
            token.price_change_1h_pct, token.price_change_24h_pct,
            token.volume_1h_change_pct, token.volume_5m_change_pct,
            token.buy_24h, token.sell_24h,
            token.volume_buy_24h_usd, token.volume_sell_24h_usd,
            token.launchpad_source,
            1 if token.graduated else 0 if token.graduated is not None else None,
            token.progress_percent, token.launchpad_creator,
            token.real_sol_reserves, token.platform_id,
            token.top10_holder_pct, token.top10_user_pct, token.creator_pct,
            token.freeze_authority,
            1 if token.transfer_fee_enabled else 0 if token.transfer_fee_enabled is not None else None,
            token.pre_market_holder_count,
            json.dumps(overview_data) if overview_data else None,
            token.list_appearances, token.time_in_tier_seconds, token.liquidity_ratio,
        ))
        self.conn.commit()

        logger.debug(f"Snapshot logged: {token.ticker} @ {stage}")

    # ── Entry Condition Logging ──────────────────────────────

    def log_entry_conditions(
        self,
        token_ca: str,
        ticker: str,
        strategy_name: str,
        score: dict,
        strategy_metrics: Optional[dict] = None,
        funnel_state: Optional[dict] = None,
    ):
        """
        Record entry conditions when a strategy fires a signal.

        Args:
            token_ca: token address
            ticker: token ticker
            strategy_name: instance label, e.g. "BBDOWN_A"
            score: output of ScoringCalculator.compute()
            strategy_metrics: RSI, SMA, bands, etc.
            funnel_state: current bucket fill levels
        """
        if self.conn is None:
            return

        now = datetime.now(timezone.utc)
        sm = strategy_metrics or {}
        fs = funnel_state or {}

        self.conn.execute("""
            INSERT INTO entry_conditions (
                token_ca, ticker, strategy_name, timestamp,
                total_score, volume_score, bb_width_score,
                vol_mc_ratio_score, mc_sweet_spot_score,
                risk_penalty, is_1h_risk_zone,
                volume_per_candle, bb_width_pct, vol_mc_ratio, market_cap,
                rsi, sma_value, upper_band, lower_band,
                price_at_entry, distance_from_lbb_pct, distance_from_ubb_pct,
                utc_hour, candles_since_full_entry,
                funnel_wide_fill, funnel_narrow_fill, funnel_full_fill
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?
            )
        """, (
            token_ca, ticker, strategy_name, now.isoformat(),
            score.get("total_score"), score.get("volume_score"),
            score.get("bb_width_score"), score.get("vol_mc_ratio_score"),
            score.get("mc_sweet_spot_score"),
            score.get("risk_penalty"),
            1 if score.get("is_1h_risk_zone") else 0,
            score.get("volume_per_candle"), score.get("bb_width_pct"),
            score.get("vol_mc_ratio"), score.get("market_cap"),
            sm.get("rsi"), sm.get("sma_value"),
            sm.get("upper_band"), sm.get("lower_band"),
            sm.get("price_at_entry"),
            sm.get("distance_from_lbb_pct"), sm.get("distance_from_ubb_pct"),
            now.hour, sm.get("candles_since_full_entry"),
            fs.get("wide_fill_pct"), fs.get("narrow_fill_pct"),
            fs.get("full_fill_pct"),
        ))
        self.conn.commit()

        logger.debug(
            f"Entry conditions logged: {ticker} {strategy_name} "
            f"score={score.get('total_score')}"
        )

    # ── OHLCV Archive ────────────────────────────────────────

    def log_candle(self, candle: Candle):
        """
        Archive a finalized candle from the WebSocket stream.
        Buffered for write efficiency — flushes every 100 candles.
        """
        if self.conn is None:
            return

        self._candle_buffer.append((
            candle.token_ca,
            candle.timeframe.value,
            candle.timestamp.isoformat(),
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume,
            candle.market_cap,
        ))

        if len(self._candle_buffer) >= self._buffer_limit:
            self._flush_candle_buffer()

    def _flush_candle_buffer(self):
        """Batch write buffered candles to SQLite."""
        if not self._candle_buffer or self.conn is None:
            return

        self.conn.executemany("""
            INSERT INTO ohlcv_archive (
                token_ca, timeframe, timestamp,
                open, high, low, close, volume, market_cap
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, self._candle_buffer)
        self.conn.commit()

        count = len(self._candle_buffer)
        self._candle_buffer.clear()
        logger.debug(f"OHLCV archive: flushed {count} candles")

    # ── Query Methods (for post-run analysis) ────────────────

    def get_snapshots(self, token_ca: str = None,
                      stage: str = None) -> List[dict]:
        """Retrieve scoring snapshots, optionally filtered."""
        query = "SELECT * FROM scoring_snapshots WHERE 1=1"
        params = []
        if token_ca:
            query += " AND token_ca = ?"
            params.append(token_ca)
        if stage:
            query += " AND stage = ?"
            params.append(stage)
        query += " ORDER BY timestamp"

        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def get_snapshot_deltas(self, token_ca: str) -> Optional[dict]:
        """
        Compute delta between narrow_entry and full_promotion snapshots.
        Returns dict of field deltas, or None if both snapshots don't exist.
        """
        narrow = self.conn.execute(
            "SELECT * FROM scoring_snapshots WHERE token_ca=? AND stage='narrow_entry' "
            "ORDER BY timestamp DESC LIMIT 1", (token_ca,)
        ).fetchone()
        full = self.conn.execute(
            "SELECT * FROM scoring_snapshots WHERE token_ca=? AND stage='full_promotion' "
            "ORDER BY timestamp DESC LIMIT 1", (token_ca,)
        ).fetchone()

        if not narrow or not full:
            return None

        delta_fields = [
            "market_cap", "liquidity", "volume_5m_usd", "volume_1h_usd",
            "volume_24h_usd", "holder_count", "unique_wallet_24h",
            "trade_1h_count", "progress_percent", "real_sol_reserves",
        ]
        deltas = {"token_ca": token_ca}
        for f in delta_fields:
            n_val = narrow[f]
            f_val = full[f]
            if n_val is not None and f_val is not None and n_val != 0:
                deltas[f"{f}_delta"] = f_val - n_val
                deltas[f"{f}_delta_pct"] = (f_val - n_val) / abs(n_val)
            else:
                deltas[f"{f}_delta"] = None
                deltas[f"{f}_delta_pct"] = None

        return deltas

    def get_entry_conditions(self, strategy_name: str = None,
                             min_score: int = None) -> List[dict]:
        """Retrieve entry conditions, optionally filtered."""
        query = "SELECT * FROM entry_conditions WHERE 1=1"
        params = []
        if strategy_name:
            query += " AND strategy_name = ?"
            params.append(strategy_name)
        if min_score is not None:
            query += " AND total_score >= ?"
            params.append(min_score)
        query += " ORDER BY timestamp"

        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def get_ohlcv(self, token_ca: str, timeframe: str = "5m",
                  limit: int = 5000) -> List[dict]:
        """Retrieve archived OHLCV candles for a token."""
        rows = self.conn.execute(
            "SELECT * FROM ohlcv_archive WHERE token_ca=? AND timeframe=? "
            "ORDER BY timestamp LIMIT ?",
            (token_ca, timeframe, limit)
        ).fetchall()
        return [dict(r) for r in rows]

    def get_stats(self) -> dict:
        """Summary stats for the scoring collection."""
        if self.conn is None:
            return {}

        snap_count = self.conn.execute(
            "SELECT COUNT(*) FROM scoring_snapshots"
        ).fetchone()[0]
        entry_count = self.conn.execute(
            "SELECT COUNT(*) FROM entry_conditions"
        ).fetchone()[0]
        candle_count = self.conn.execute(
            "SELECT COUNT(*) FROM ohlcv_archive"
        ).fetchone()[0]
        unique_tokens = self.conn.execute(
            "SELECT COUNT(DISTINCT token_ca) FROM scoring_snapshots"
        ).fetchone()[0]

        return {
            "snapshots": snap_count,
            "entry_conditions": entry_count,
            "candles_archived": candle_count,
            "unique_tokens_tracked": unique_tokens,
            "pending_candle_buffer": len(self._candle_buffer),
        }
