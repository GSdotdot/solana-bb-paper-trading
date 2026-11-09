"""
Database Manager (Layer 5) — V6 Thread-Safe
=============================================
SQLite persistence for all trading data.

V6 CHANGE: All blocking SQLite operations (flush_writes, log_snapshot,
reconstruct_cycles) are designed to be called via asyncio.run_in_executor()
from the async layer. The methods themselves remain synchronous but are
no longer called directly from the event loop — main.py wraps them.

This eliminates the silent event loop freeze caused by SQLite commits
blocking the async event loop for extended periods under load.

Tables:
  - trades:              Every execution (buy/sell) with full cost breakdown
  - portfolio_snapshots:  Periodic state captures per strategy
  - events:              System events (token add/remove, cooldowns, errors)
  - cycles:              Reconstructed trade cycles (entry + all exits = 1 cycle)
"""

import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import logging

from paper_trading.execution.executor import TradeRecord, PortfolioManager

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 3  # V7.1: +open_positions (A1 crash-durable persistence)


class DatabaseManager:
    """
    Manages SQLite connection and all write/read operations.
    Thread-safe via sqlite3's serialized mode.

    V6: flush_writes() and reconstruct_cycles() log their execution
    time so the heartbeat can detect slow operations.
    """

    def __init__(self, db_path: str = "paper_trading.db"):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self._trade_buffer: List[tuple] = []
        self._event_buffer: List[tuple] = []
        self._write_buffer_limit: int = 50
        # V6: Timing diagnostics
        self.last_flush_ms: float = 0
        self.last_cycle_rebuild_ms: float = 0
        self.total_flushes: int = 0

    def open(self):
        """Open connection and create tables."""
        self.conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            timeout=30.0,
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=30000")
        self._create_tables()
        self._trade_buffer.clear()
        self._event_buffer.clear()
        logger.info(f"Database opened: {self.db_path}")

    def close(self):
        if self.conn:
            self.flush_writes()
            self.conn.close()
            self.conn = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args):
        self.close()

    # ════════════════════════════════════════════════════════════
    # SCHEMA
    # ════════════════════════════════════════════════════════════

    def _create_tables(self):
        c = self.conn
        c.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id        TEXT PRIMARY KEY,
                strategy_name   TEXT NOT NULL,
                token_ca        TEXT NOT NULL,
                token_ticker    TEXT NOT NULL,
                signal_type     TEXT NOT NULL,
                timestamp       TEXT NOT NULL,

                -- Pricing
                fill_price      REAL NOT NULL,
                execution_price REAL NOT NULL,
                candle_close    REAL NOT NULL,
                market_cap      REAL NOT NULL,

                -- Sizing
                usd_amount      REAL NOT NULL,
                token_quantity  REAL NOT NULL,

                -- Costs
                slippage_pct    REAL NOT NULL,
                slippage_usd    REAL NOT NULL,
                dex_fee_usd     REAL NOT NULL,
                priority_fee_usd REAL NOT NULL,
                total_cost_usd  REAL NOT NULL,
                net_usd         REAL NOT NULL,

                -- Portfolio state after
                cash_after      REAL NOT NULL,
                positions_after INTEGER NOT NULL,

                -- Indicators snapshot
                ind_sma         REAL,
                ind_bb_upper    REAL,
                ind_bb_lower    REAL,
                ind_bb_width    REAL,
                ind_rsi         REAL,
                ind_vol_avg     REAL,

                -- Extra
                sell_pct        REAL DEFAULT 0.0,
                reason          TEXT DEFAULT '',

                -- Cycle linkage
                cycle_id        TEXT
            );

            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_name   TEXT NOT NULL,
                timestamp       TEXT NOT NULL,
                cash_balance    REAL NOT NULL,
                open_positions  INTEGER NOT NULL,
                deployed_capital REAL NOT NULL,
                total_value     REAL NOT NULL,
                total_trades    INTEGER NOT NULL,
                total_cycles    INTEGER NOT NULL,
                peak_value      REAL NOT NULL,
                total_fees_paid REAL NOT NULL,
                total_slippage  REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS events (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp       TEXT NOT NULL,
                strategy_name   TEXT DEFAULT '',
                token_ca        TEXT DEFAULT '',
                event_type      TEXT NOT NULL,
                details         TEXT DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS cycles (
                cycle_id        TEXT PRIMARY KEY,
                strategy_name   TEXT NOT NULL,
                token_ca        TEXT NOT NULL,
                token_ticker    TEXT NOT NULL,

                -- Entry
                entry_trade_id  TEXT NOT NULL,
                entry_timestamp TEXT NOT NULL,
                entry_fill_price REAL NOT NULL,
                entry_exec_price REAL NOT NULL,
                entry_market_cap REAL NOT NULL,
                entry_tokens    REAL NOT NULL,
                entry_invested  REAL NOT NULL,

                -- Exit (final)
                exit_type       TEXT NOT NULL,
                exit_timestamp  TEXT NOT NULL,
                exit_fill_price REAL NOT NULL,
                exit_exec_price REAL NOT NULL,
                exit_market_cap REAL NOT NULL,

                -- Partial (BBDOWN SMA partial exit)
                had_partial     INTEGER DEFAULT 0,
                partial_trade_id TEXT,
                partial_timestamp TEXT,
                partial_fill_price REAL,
                partial_exec_price REAL,
                partial_tokens  REAL,
                partial_net_usd REAL,

                -- Totals
                total_proceeds  REAL NOT NULL,
                total_costs     REAL NOT NULL,
                net_pnl         REAL NOT NULL,
                pnl_pct         REAL NOT NULL,
                duration_seconds INTEGER NOT NULL,
                candles_in_trade INTEGER DEFAULT 0,

                -- Trade quality
                mfe_price       REAL DEFAULT 0.0,
                mae_price       REAL DEFAULT 0.0
            );

            -- Indexes
            CREATE INDEX IF NOT EXISTS idx_trades_strategy
                ON trades(strategy_name, timestamp);
            CREATE INDEX IF NOT EXISTS idx_trades_token
                ON trades(token_ca, timestamp);
            CREATE INDEX IF NOT EXISTS idx_trades_cycle
                ON trades(cycle_id);
            CREATE INDEX IF NOT EXISTS idx_snapshots_strategy
                ON portfolio_snapshots(strategy_name, timestamp);
            CREATE INDEX IF NOT EXISTS idx_cycles_strategy
                ON cycles(strategy_name, exit_timestamp);
            CREATE INDEX IF NOT EXISTS idx_events_type
                ON events(event_type, timestamp);

            -- A1 (V7.1): crash-durable open-position persistence.
            -- Written SYNCHRONOUSLY on every PortfolioManager mutation (not
            -- buffered), so a kill-9 between a mutation and the next flush
            -- cannot orphan an open buy (Issue B). Restored on startup.
            -- All timestamps ISO-8601 TEXT (never unix epoch).
            CREATE TABLE IF NOT EXISTS open_positions (
                strategy_name           TEXT NOT NULL,
                token_ca                TEXT NOT NULL,
                entry_timestamp         TEXT NOT NULL,
                entry_price             REAL NOT NULL,
                tokens_held             REAL NOT NULL,
                invested_usd            REAL NOT NULL,
                entry_market_cap        REAL NOT NULL,
                sma_partial_done        INTEGER NOT NULL DEFAULT 0,
                sma_sl_pending          INTEGER,
                last_updated_timestamp  TEXT NOT NULL,
                PRIMARY KEY (strategy_name, token_ca)
            );

            -- Schema version tracking
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        c.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            ("schema_version", str(SCHEMA_VERSION))
        )
        c.commit()

    # ════════════════════════════════════════════════════════════
    # TRADE LOGGING
    # ════════════════════════════════════════════════════════════

    def log_trade(self, trade: TradeRecord):
        """Buffer a trade for batch writing."""
        ind = trade.indicators
        self._trade_buffer.append((
            trade.trade_id, trade.strategy_name, trade.token_ca, trade.token_ticker,
            trade.signal_type, trade.timestamp.isoformat() if trade.timestamp else "",
            trade.fill_price, trade.execution_price, trade.candle_close, trade.market_cap,
            trade.usd_amount, trade.token_quantity, trade.slippage_pct, trade.slippage_usd,
            trade.dex_fee_usd, trade.priority_fee_usd, trade.total_cost_usd, trade.net_usd,
            trade.cash_after, trade.positions_after,
            ind.sma if ind else None, ind.bb_upper if ind else None,
            ind.bb_lower if ind else None, ind.bb_width_pct if ind else None,
            ind.rsi if ind else None, ind.volume_avg_n if ind else None,
            trade.sell_pct, trade.reason,
        ))
        if len(self._trade_buffer) >= self._write_buffer_limit:
            self.flush_writes()

    def log_trades(self, trades: List[TradeRecord]):
        for t in trades:
            self.log_trade(t)

    def log_event(
        self, event_type: str, timestamp: datetime,
        strategy_name: str = "", token_ca: str = "", details: str = "",
    ):
        """Buffer an event for batch writing."""
        self._event_buffer.append((
            timestamp.isoformat(), strategy_name, token_ca, event_type, details
        ))
        if len(self._event_buffer) >= self._write_buffer_limit:
            self.flush_writes()

    def flush_writes(self):
        """
        Persist all buffered trades and events in a single transaction.

        V6: Tracks execution time. When called via run_in_executor(),
        this runs in a thread pool and never blocks the async event loop.
        """
        if not self._trade_buffer and not self._event_buffer:
            return

        t0 = time.monotonic()
        try:
            if self._trade_buffer:
                self.conn.executemany("""
                    INSERT OR IGNORE INTO trades (
                        trade_id, strategy_name, token_ca, token_ticker, signal_type,
                        timestamp, fill_price, execution_price, candle_close, market_cap,
                        usd_amount, token_quantity, slippage_pct, slippage_usd,
                        dex_fee_usd, priority_fee_usd, total_cost_usd, net_usd,
                        cash_after, positions_after,
                        ind_sma, ind_bb_upper, ind_bb_lower, ind_bb_width, ind_rsi, ind_vol_avg,
                        sell_pct, reason
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, self._trade_buffer)

            if self._event_buffer:
                self.conn.executemany("""
                    INSERT INTO events (timestamp, strategy_name, token_ca, event_type, details)
                    VALUES (?,?,?,?,?)
                """, self._event_buffer)

            self.conn.commit()

            trades_flushed = len(self._trade_buffer)
            events_flushed = len(self._event_buffer)
            self._trade_buffer.clear()
            self._event_buffer.clear()
            self.total_flushes += 1

            elapsed_ms = (time.monotonic() - t0) * 1000
            self.last_flush_ms = elapsed_ms

            if elapsed_ms > 500:
                logger.warning(
                    f"DB flush slow: {elapsed_ms:.0f}ms "
                    f"({trades_flushed} trades, {events_flushed} events)"
                )
            elif trades_flushed > 0:
                logger.debug(
                    f"DB flush: {trades_flushed} trades, {events_flushed} events "
                    f"({elapsed_ms:.0f}ms)"
                )

        except sqlite3.OperationalError as e:
            logger.warning(f"DB flush failed (will retry next cycle): {e}")

    @property
    def pending_writes(self) -> int:
        return len(self._trade_buffer) + len(self._event_buffer)

    # ════════════════════════════════════════════════════════════
    # PORTFOLIO SNAPSHOTS
    # ════════════════════════════════════════════════════════════

    def log_snapshot(self, portfolio: PortfolioManager, timestamp: datetime):
        """Capture current portfolio state. Commits immediately."""
        self.flush_writes()
        self.conn.execute("""
            INSERT INTO portfolio_snapshots (
                strategy_name, timestamp, cash_balance, open_positions,
                deployed_capital, total_value, total_trades, total_cycles,
                peak_value, total_fees_paid, total_slippage
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            portfolio.strategy_name, timestamp.isoformat(),
            portfolio.cash_balance, portfolio.open_position_count,
            portfolio.deployed_capital, portfolio.total_value,
            portfolio.total_trades, portfolio.total_cycles,
            portfolio.peak_value, portfolio.total_fees_paid,
            portfolio.total_slippage_cost,
        ))
        self.conn.commit()

    # ════════════════════════════════════════════════════════════
    # OPEN POSITIONS (A1 — crash-durable open-position persistence)
    # ════════════════════════════════════════════════════════════
    #
    # Unlike trades/events (buffered, flushed in batches), open-position state
    # is written SYNCHRONOUSLY and committed at every PortfolioManager mutation.
    # Restart=always means a kill-9 can land between a mutation and the next
    # buffered flush; buffered position state would be lost and the restored
    # portfolio would orphan the open buy (Issue B). So this path is the one
    # place the buffered-write pattern is deliberately NOT used.
    #
    # Each mutation writes in ONE transaction: the open_positions row change
    # AND a fresh portfolio_snapshot capturing post-mutation cash. That keeps
    # the latest snapshot's cash_balance always consistent with the
    # open_positions rows, so restore (cash from snapshot, positions from
    # open_positions) stays coherent even on an ungraceful kill — the periodic
    # snapshot alone would be stale relative to the just-mutated positions.

    def _snapshot_insert_nocommit(self, portfolio: PortfolioManager, ts: datetime):
        """Insert a portfolio_snapshots row WITHOUT committing (caller commits
        as part of the atomic open-position transaction)."""
        self.conn.execute("""
            INSERT INTO portfolio_snapshots (
                strategy_name, timestamp, cash_balance, open_positions,
                deployed_capital, total_value, total_trades, total_cycles,
                peak_value, total_fees_paid, total_slippage
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            portfolio.strategy_name, ts.isoformat(),
            portfolio.cash_balance, portfolio.open_position_count,
            portfolio.deployed_capital, portfolio.total_value,
            portfolio.total_trades, portfolio.total_cycles,
            portfolio.peak_value, portfolio.total_fees_paid,
            portfolio.total_slippage_cost,
        ))

    def upsert_open_position(
        self, position, portfolio: PortfolioManager, ts: datetime,
        sma_sl_pending: int = 0, sma_partial_done: int = 0,
    ):
        """Insert/replace one open position AND snapshot cash, atomically.

        Used for open_position (fresh: both flags 0) and record_partial_sell
        (sma_partial_done=1, reduced tokens_held). `position.tokens_remaining`
        is the live held quantity (decremented on partials).
        """
        self.conn.execute("""
            INSERT OR REPLACE INTO open_positions (
                strategy_name, token_ca, entry_timestamp, entry_price,
                tokens_held, invested_usd, entry_market_cap,
                sma_partial_done, sma_sl_pending, last_updated_timestamp
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            position.strategy_name, position.token_ca,
            position.entry_timestamp.isoformat() if position.entry_timestamp else ts.isoformat(),
            position.entry_price, position.tokens_remaining,
            position.invested_usd, position.entry_market_cap,
            int(sma_partial_done), int(sma_sl_pending), ts.isoformat(),
        ))
        self._snapshot_insert_nocommit(portfolio, ts)
        self.conn.commit()

    def delete_open_position(
        self, strategy_name: str, token_ca: str,
        portfolio: PortfolioManager, ts: datetime,
    ):
        """Delete one open position (full exit) AND snapshot cash, atomically."""
        self.conn.execute(
            "DELETE FROM open_positions WHERE strategy_name=? AND token_ca=?",
            (strategy_name, token_ca),
        )
        self._snapshot_insert_nocommit(portfolio, ts)
        self.conn.commit()

    def update_open_position_flags(
        self, strategy_name: str, token_ca: str, ts: datetime,
        sma_sl_pending: int = 0, sma_partial_done: int = 0,
    ):
        """Persist an exit-flag change that happened WITHOUT a cash mutation
        (e.g. BBUP sma_sl_pending set when a candle closes below SMA). Cash is
        unchanged here, so no snapshot is written. Committed immediately."""
        self.conn.execute("""
            UPDATE open_positions
            SET sma_sl_pending=?, sma_partial_done=?, last_updated_timestamp=?
            WHERE strategy_name=? AND token_ca=?
        """, (int(sma_sl_pending), int(sma_partial_done), ts.isoformat(),
              strategy_name, token_ca))
        self.conn.commit()

    def get_open_positions(self, strategy_name: str = None) -> List[dict]:
        """Read all durable open positions (optionally one strategy)."""
        if strategy_name:
            rows = self.conn.execute(
                "SELECT * FROM open_positions WHERE strategy_name=?",
                (strategy_name,),
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM open_positions").fetchall()
        return [dict(r) for r in rows]

    def get_ticker_for(self, strategy_name: str, token_ca: str) -> str:
        """Recover a display ticker for a restored position from the most
        recent trade row (open_positions stores no ticker — schema is kept
        exactly as specified). Empty string if no trade row exists."""
        r = self.conn.execute(
            "SELECT token_ticker FROM trades WHERE strategy_name=? AND token_ca=? "
            "ORDER BY timestamp DESC LIMIT 1",
            (strategy_name, token_ca),
        ).fetchone()
        return r["token_ticker"] if r else ""

    # ════════════════════════════════════════════════════════════
    # CYCLE RECONSTRUCTION
    # ════════════════════════════════════════════════════════════

    def reconstruct_cycles(self, strategy_name: str = None):
        """
        Build cycle records from raw trades.
        V6: Tracks execution time for diagnostics.
        """
        t0 = time.monotonic()

        where = "WHERE strategy_name = ?" if strategy_name else ""
        params = (strategy_name,) if strategy_name else ()

        rows = self.conn.execute(f"""
            SELECT * FROM trades {where}
            ORDER BY strategy_name, token_ca, timestamp
        """, params).fetchall()

        cycles_built = 0
        entry = None
        partial = None

        for row in rows:
            sig = row["signal_type"]

            if sig == "entry":
                entry = row
                partial = None

            elif sig == "exit_sma_partial" and entry:
                partial = row

            elif sig.startswith("exit_") and entry:
                cycle_id = entry["trade_id"]

                # V7.1 FIX: entry net_usd sign was corrected (BUY now stored
                # negative). Use abs() so the cycle math is identical whether
                # the entry row predates the fix (+cost) or postdates it
                # (-cost). entry_invested is always the positive cost basis.
                entry_invested = abs(entry["net_usd"])
                partial_net = partial["net_usd"] if partial else 0.0
                final_net = row["net_usd"]
                total_proceeds = partial_net + final_net
                total_costs = entry["total_cost_usd"] + (
                    partial["total_cost_usd"] if partial else 0.0
                ) + row["total_cost_usd"]
                net_pnl = total_proceeds - entry_invested
                pnl_pct = (net_pnl / entry_invested * 100) if entry_invested > 0 else 0.0

                entry_ts = datetime.fromisoformat(entry["timestamp"])
                exit_ts = datetime.fromisoformat(row["timestamp"])
                duration = int((exit_ts - entry_ts).total_seconds())

                self.conn.execute("""
                    INSERT OR REPLACE INTO cycles (
                        cycle_id, strategy_name, token_ca, token_ticker,
                        entry_trade_id, entry_timestamp, entry_fill_price,
                        entry_exec_price, entry_market_cap, entry_tokens, entry_invested,
                        exit_type, exit_timestamp, exit_fill_price, exit_exec_price,
                        exit_market_cap,
                        had_partial, partial_trade_id, partial_timestamp,
                        partial_fill_price, partial_exec_price, partial_tokens,
                        partial_net_usd,
                        total_proceeds, total_costs, net_pnl, pnl_pct,
                        duration_seconds
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    cycle_id, entry["strategy_name"], entry["token_ca"],
                    entry["token_ticker"],
                    entry["trade_id"], entry["timestamp"], entry["fill_price"],
                    entry["execution_price"], entry["market_cap"],
                    entry["token_quantity"], entry_invested,
                    row["signal_type"], row["timestamp"], row["fill_price"],
                    row["execution_price"], row["market_cap"],
                    1 if partial else 0,
                    partial["trade_id"] if partial else None,
                    partial["timestamp"] if partial else None,
                    partial["fill_price"] if partial else None,
                    partial["execution_price"] if partial else None,
                    partial["token_quantity"] if partial else None,
                    partial_net if partial else None,
                    total_proceeds, total_costs, net_pnl, pnl_pct, duration,
                ))

                self.conn.execute(
                    "UPDATE trades SET cycle_id=? WHERE trade_id=?",
                    (cycle_id, entry["trade_id"])
                )
                if partial:
                    self.conn.execute(
                        "UPDATE trades SET cycle_id=? WHERE trade_id=?",
                        (cycle_id, partial["trade_id"])
                    )
                self.conn.execute(
                    "UPDATE trades SET cycle_id=? WHERE trade_id=?",
                    (cycle_id, row["trade_id"])
                )

                cycles_built += 1
                entry = None
                partial = None

        self.conn.commit()

        elapsed_ms = (time.monotonic() - t0) * 1000
        self.last_cycle_rebuild_ms = elapsed_ms

        if elapsed_ms > 1000:
            logger.warning(
                f"Cycle rebuild slow: {elapsed_ms:.0f}ms, "
                f"{cycles_built} cycles from {len(rows)} trades"
            )
        else:
            logger.info(
                f"Reconstructed {cycles_built} cycles ({elapsed_ms:.0f}ms)"
            )
        return cycles_built

    # ════════════════════════════════════════════════════════════
    # READ QUERIES
    # ════════════════════════════════════════════════════════════

    def get_trades(self, strategy_name: str = None, token_ca: str = None,
                   limit: int = 1000) -> List[dict]:
        clauses, params = [], []
        if strategy_name:
            clauses.append("strategy_name = ?"); params.append(strategy_name)
        if token_ca:
            clauses.append("token_ca = ?"); params.append(token_ca)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.conn.execute(
            f"SELECT * FROM trades {where} ORDER BY timestamp LIMIT ?",
            params + [limit]
        ).fetchall()
        return [dict(r) for r in rows]

    def get_cycles(self, strategy_name: str = None, limit: int = 1000) -> List[dict]:
        clauses, params = [], []
        if strategy_name:
            clauses.append("strategy_name = ?"); params.append(strategy_name)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.conn.execute(
            f"SELECT * FROM cycles {where} ORDER BY exit_timestamp LIMIT ?",
            params + [limit]
        ).fetchall()
        return [dict(r) for r in rows]

    def get_snapshots(self, strategy_name: str = None, limit: int = 5000) -> List[dict]:
        clauses, params = [], []
        if strategy_name:
            clauses.append("strategy_name = ?"); params.append(strategy_name)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.conn.execute(
            f"SELECT * FROM portfolio_snapshots {where} ORDER BY timestamp LIMIT ?",
            params + [limit]
        ).fetchall()
        return [dict(r) for r in rows]

    def get_events(self, event_type: str = None, limit: int = 1000) -> List[dict]:
        clauses, params = [], []
        if event_type:
            clauses.append("event_type = ?"); params.append(event_type)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self.conn.execute(
            f"SELECT * FROM events {where} ORDER BY timestamp LIMIT ?",
            params + [limit]
        ).fetchall()
        return [dict(r) for r in rows]

    def get_trade_count(self, strategy_name: str = None) -> int:
        if strategy_name:
            r = self.conn.execute(
                "SELECT COUNT(*) FROM trades WHERE strategy_name=?", (strategy_name,)
            ).fetchone()
        else:
            r = self.conn.execute("SELECT COUNT(*) FROM trades").fetchone()
        return r[0]

    def get_cycle_count(self, strategy_name: str = None) -> int:
        if strategy_name:
            r = self.conn.execute(
                "SELECT COUNT(*) FROM cycles WHERE strategy_name=?", (strategy_name,)
            ).fetchone()
        else:
            r = self.conn.execute("SELECT COUNT(*) FROM cycles").fetchone()
        return r[0]

    def get_latest_snapshots(self) -> Dict[str, dict]:
        rows = self.conn.execute("""
            SELECT * FROM portfolio_snapshots
            WHERE id IN (
                SELECT MAX(id) FROM portfolio_snapshots
                GROUP BY strategy_name
            )
        """).fetchall()
        return {row["strategy_name"]: dict(row) for row in rows}
