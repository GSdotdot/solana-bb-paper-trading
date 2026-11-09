"""
Data Engine Manager (Layer 2)
==============================
Top-level coordinator for all data operations.
Bridges the gap between the raw data-provider APIs and the Orchestrator.

Responsibilities:
  1. WebSocket lifecycle (connect, subscribe, reconnect)
  2. Candle delivery → Orchestrator.on_candle_received()
  3. Historical backfill on token discovery
  4. Market cap via the token-stats stream (REST fallback)
  5. API credit tracking (WS bytes + REST calls) for the monthly budget

Integration:
  Orchestrator calls:
    data_engine.add_token(ca, ticker)    → subscribe WS + backfill
    data_engine.remove_token(ca)         → unsubscribe WS
    data_engine.start()                  → connect + listen loop
    data_engine.stop()                   → graceful disconnect

  Data Engine calls:
    orchestrator.on_candle_received()    → when candle finalizes
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from paper_trading.core.models import Candle, Timeframe, TickData
from paper_trading.config.params import param
from paper_trading.data_engine.websocket_client import DataProviderWebSocket
from paper_trading.data_engine.rest_client import DataProviderRESTClient

logger = logging.getLogger(__name__)

CandleCallback = Callable[[Candle], None]
TickCallback = Callable[[TickData], None]

# Timeframes our strategies need
STRATEGY_TIMEFRAMES = [Timeframe.ONE_MIN, Timeframe.FIVE_MIN]


class DataEngine:
    """
    Manages all data feeds for the trading system.
    V5: Now passes tick data to orchestrator for real-time signal detection.
    """

    def __init__(
        self,
        api_key: str,
        on_candle: CandleCallback,
        on_tick: Optional[TickCallback] = None,
        backfill_candles: int = param("data_engine.backfill_candles"),
        market_cap_poll_interval: int = param("data_engine.market_cap_poll_interval_seconds"),
    ):
        self.api_key = api_key
        self.on_candle = on_candle
        self.on_tick = on_tick
        self.backfill_candles = backfill_candles
        self.market_cap_poll_interval = market_cap_poll_interval

        # Components
        self.ws_client = DataProviderWebSocket(
            api_key, self._on_ws_candle, on_tick=self._on_ws_tick
        )
        self.rest_client = DataProviderRESTClient(api_key)

        # State
        self._running = False
        self._tracked_tokens: Dict[str, str] = {}  # ca → ticker
        self._mcap_poll_task: Optional[asyncio.Task] = None

    # ── Lifecycle ────────────────────────────────────────────

    async def start(self):
        """Start WebSocket connection and background tasks."""
        self._running = True
        logger.info("Data Engine starting...")

        # Connect WebSocket
        await self.ws_client.connect()

        # Start tick queue consumer (V5.1 fix: async tick processing)
        self.ws_client.start_tick_consumer()

        # Start market cap poller as REST fallback
        # Primary mcap source is the token-stats stream over the WS.
        # This REST loop only fires if the stream isn't providing data.
        self._mcap_poll_task = asyncio.create_task(
            self._market_cap_poll_loop()
        )

        logger.info("Data Engine started")

    async def run(self):
        """Run the main listen loop (blocks until stop)."""
        await self.ws_client.listen()

    async def force_ws_reconnect(self):
        """V7.1: passthrough for the heartbeat watchdog to force a WS
        reconnect when the feed has gone silent past threshold."""
        await self.ws_client.force_reconnect()

    def seconds_since_last_ws_message(self) -> float:
        """V7.1: passthrough for the heartbeat watchdog."""
        return self.ws_client.seconds_since_last_message()

    async def stop(self):
        """Graceful shutdown."""
        self._running = False
        logger.info("Data Engine stopping...")

        # Cancel market cap poller
        if self._mcap_poll_task and not self._mcap_poll_task.done():
            self._mcap_poll_task.cancel()
            try:
                await self._mcap_poll_task
            except asyncio.CancelledError:
                pass

        # Stop tick consumer (V5.1 fix: drain queue before disconnect)
        await self.ws_client.stop_tick_consumer()

        # Disconnect WebSocket (flushes remaining candles)
        await self.ws_client.disconnect()

        # Close REST client session
        await self.rest_client.close()

        logger.info("Data Engine stopped")

    # ── Token management ─────────────────────────────────────

    async def add_token(self, token_ca: str, ticker: str):
        """
        Add a token to live monitoring.
        1. Subscribe to WebSocket OHLCV for all needed timeframes
        2. Fetch historical candles for backfill
        3. Return backfill candles for strategy seeding
        """
        self._tracked_tokens[token_ca] = ticker

        # Subscribe WebSocket
        await self.ws_client.add_token(token_ca, STRATEGY_TIMEFRAMES)

        logger.info(
            f"Token added to data engine: {ticker} ({token_ca[:12]}...)"
        )

    async def add_token_with_backfill(
        self, token_ca: str, ticker: str,
    ) -> tuple:
        """
        Add token and fetch history. Returns (history_5m, history_1m).
        The orchestrator passes these to on_token_discovered().
        
        🔧 NOTE: Backfill currently returns empty lists (REST placeholder).
        When aiohttp is integrated, this will return actual candle history.
        """
        await self.add_token(token_ca, ticker)

        # Fetch history for both timeframes
        result_5m = await self.rest_client.fetch_ohlcv(
            token_ca, Timeframe.FIVE_MIN, self.backfill_candles
        )
        result_1m = await self.rest_client.fetch_ohlcv(
            token_ca, Timeframe.ONE_MIN, self.backfill_candles
        )

        return (result_5m.candles, result_1m.candles)

    async def remove_token(self, token_ca: str):
        """Remove a token from live monitoring."""
        self._tracked_tokens.pop(token_ca, None)
        await self.ws_client.remove_token(token_ca)

    # ── Internal callbacks ───────────────────────────────────

    def _on_ws_candle(self, candle: Candle):
        """
        Called by WebSocket client when a candle finalizes.
        Routes to the orchestrator's callback.
        """
        self.on_candle(candle)

    def _on_ws_tick(self, tick: TickData):
        """
        V5: Called by WebSocket client on every price update.
        Routes to orchestrator for real-time strategy signal detection.
        """
        if self.on_tick is not None:
            self.on_tick(tick)

    # ── Market cap polling (✅ Q4 CONFIRMED: REST fallback) ──────

    async def _market_cap_poll_loop(self):
        """
        Periodically fetch market caps for tracked tokens via REST.

        Market cap is NOT in streaming price events.
        Primary source: the token-stats stream over the WebSocket.
        This REST loop is a FALLBACK — it only fetches for tokens
        that don't have a recent mcap from the stream.

        Credit logic: REST polling costs credits per token per poll, so it
        scales with tracked tokens × poll frequency; with the stream working
        the loop skips every token and spends ~nothing.
        """
        while self._running:
            try:
                await asyncio.sleep(self.market_cap_poll_interval)
                if not self._running:
                    break

                tokens = list(self._tracked_tokens.keys())
                if not tokens:
                    continue

                # Only poll tokens missing from WS mcap cache
                tokens_needing_poll = [
                    ca for ca in tokens
                    if ca not in self.ws_client._market_cap_cache
                ]

                if not tokens_needing_poll:
                    continue  # token-stats stream is feeding all mcaps

                mcaps = await self.rest_client.fetch_market_caps_batch(
                    tokens_needing_poll
                )
                for ca, mcap in mcaps.items():
                    self.ws_client.update_market_cap(ca, mcap)

                logger.debug(
                    f"Market cap REST fallback: "
                    f"{len(mcaps)}/{len(tokens_needing_poll)} tokens updated "
                    f"({len(tokens) - len(tokens_needing_poll)} via WS)"
                )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Market cap poll error: {e}")

    # ── Stats & monitoring ───────────────────────────────────

    def get_stats(self) -> dict:
        ws_stats = self.ws_client.get_stats()
        rest_stats = self.rest_client.get_stats()
        return {
            "running": self._running,
            "tracked_tokens": len(self._tracked_tokens),
            "websocket": ws_stats,
            "rest": rest_stats,
            "total_credits": round(
                ws_stats["total_credits"] + rest_stats["total_credits_consumed"], 2
            ),
        }
