"""
Data Provider WebSocket Client (Layer 2)
========================================
Manages the real-time OHLCV feed from the market-data provider's WebSocket
API. Provider URL, subprotocol, message types, query syntax and per-byte
credit cost are redacted (env `DATA_PROVIDER_WS_URL` + config
`data_provider.ws`); the client logic below is provider-agnostic.

Protocol properties the design relies on:
  - Subscribe with a single-token message or a batched multi-token query
    (batch size capped by the provider)
  - A batched subscription is a FULL REPLACEMENT, not incremental —
    the entire token list is resent on every change
  - Streaming price events carry the in-progress candle (o/h/l/c/v + interval)
  - Billing is per byte received → credits tracked per message

Behaviour confirmed against the live feed:
  CANDLE BOUNDARY — there is no distinct close event. When a new interval
      timestamp appears, the previous candle is finalized.
  MARKET CAP — not in price events. A separate token-stats stream over the
      same socket feeds a market-cap cache (cheaper than REST polling).
  VOLUME — streamed volume is in base-token units; converted to USD via
      the close price.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Set

import os

from paper_trading.config.params import param
from paper_trading.core.models import Candle, Timeframe, TickData

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════

WS = param("data_provider.ws")                       # protocol adapter (redacted)
FIELDS = param("data_provider.response_fields")
WS_URL = os.environ.get("DATA_PROVIDER_WS_URL", "")
WS_PROTOCOL = WS["subprotocol"]
MAX_COMPLEX_ADDRESSES = WS["max_batch_clauses"]
CREDIT_PER_BYTE = WS["credit_per_byte"]               # billing is per byte received


def _fill(template, **kw):
    """Recursively format a message template (dict/list/str) from config."""
    if isinstance(template, dict):
        return {k: _fill(v, **kw) for k, v in template.items()}
    if isinstance(template, list):
        return [_fill(v, **kw) for v in template]
    if isinstance(template, str):
        return template.format(**kw)
    return template

RECONNECT_BASE_DELAY = 2.0     # seconds
RECONNECT_MAX_DELAY = 60.0     # seconds
PING_INTERVAL = 30.0           # seconds
PONG_TIMEOUT = 10.0            # seconds

# V7.1 FIX: recv() timeout guards against the `websockets` library hanging
# inside close_exc when a connection half-dies (the GIL-freeze bug of
# 2026-05-27). If no message arrives within RECV_TIMEOUT, we force a
# reconnect instead of blocking the event loop forever.
RECV_TIMEOUT = 60.0            # seconds — max wait for a single WS message

# Tick queue settings (V5.1 fix: decouple tick emission from processing)
TICK_QUEUE_MAX_SIZE = 500       # Max pending ticks; overflow = drop oldest
TICK_THROTTLE_SECONDS = 0.5    # Max 1 tick per token per this interval


def _timeframe_to_chart_type(tf: Timeframe) -> str:
    """Map our Timeframe enum to the provider's interval string."""
    return {
        Timeframe.ONE_MIN: "1m",
        Timeframe.FIVE_MIN: "5m",
    }[tf]


def _chart_type_to_timeframe(ct: str) -> Timeframe:
    """Map the provider's interval string to our Timeframe enum."""
    return {
        "1m": Timeframe.ONE_MIN,
        "5m": Timeframe.FIVE_MIN,
    }.get(ct, Timeframe.FIVE_MIN)


def _interval_seconds(tf: Timeframe) -> int:
    return {Timeframe.ONE_MIN: 60, Timeframe.FIVE_MIN: 300}[tf]


# ═══════════════════════════════════════════════════════════════
# CANDLE ASSEMBLER
# ═══════════════════════════════════════════════════════════════

@dataclass
class LiveCandle:
    """
    Tracks an in-progress candle from streaming updates.
    
    The provider sends partial updates within each interval — o/h/l/c/v
    update in real-time as transactions occur. We accumulate these
    and emit a final Candle object when the interval rolls over.

    Candle boundary = new interval timestamp (no distinct close event).
    """
    token_ca: str
    timeframe: Timeframe
    interval_start: int = 0     # interval start (unix seconds) of current candle
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    market_cap: float = 0.0     # Not in price events. Fed via token-stats stream or REST.
    update_count: int = 0
    last_update_time: float = 0.0

    def update(self, data: dict):
        """Apply a streaming price update to this candle."""
        self.open = data.get(FIELDS["candle_open"], self.open)
        self.high = data.get(FIELDS["candle_high"], self.high)
        self.low = data.get(FIELDS["candle_low"], self.low)
        self.close = data.get(FIELDS["candle_close"], self.close)
        self.volume = data.get(FIELDS["candle_volume"], self.volume)
        self.interval_start = data.get(FIELDS["candle_time"], self.interval_start)
        self.update_count += 1
        self.last_update_time = time.time()

    def to_candle(self) -> Candle:
        """Convert to finalized Candle for strategy consumption."""
        ts = datetime.fromtimestamp(self.interval_start, tz=timezone.utc)

        # Streamed volume is in base-token units (confirmed against the live
        # feed). Convert to USD using the close price.
        usd_volume = self.volume * self.close

        return Candle(
            token_ca=self.token_ca,
            timeframe=self.timeframe,
            timestamp=ts,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=usd_volume,
            market_cap=self.market_cap,
        )


class CandleAssembler:
    """
    Assembles streaming price updates into finalized Candle objects.
    
    One assembler per (token_ca, timeframe) pair.
    Emits a completed candle when the interval rolls over.
    """

    def __init__(self):
        # Key: (token_ca, timeframe) → LiveCandle
        self._active: Dict[tuple, LiveCandle] = {}

    def process_update(self, token_ca: str, timeframe: Timeframe,
                       data: dict) -> Optional[Candle]:
        """
        Process a streaming price message. Returns a finalized Candle
        if the interval rolled over, otherwise None.

        No distinct close event: a new interval timestamp means the
        previous candle is final.
        """
        key = (token_ca, timeframe)
        new_unix = data.get(FIELDS["candle_time"], 0)

        current = self._active.get(key)

        if current is None:
            # First update for this token/timeframe
            current = LiveCandle(token_ca=token_ca, timeframe=timeframe)
            current.update(data)
            self._active[key] = current
            return None

        # Check if we've moved to a new candle interval
        if new_unix > current.interval_start and current.interval_start > 0:
            # New interval = previous candle finalized.
            # Finalize the previous candle
            completed = current.to_candle()

            # Start new candle
            new_candle = LiveCandle(token_ca=token_ca, timeframe=timeframe)
            new_candle.update(data)
            self._active[key] = new_candle

            return completed

        # Same interval — update in place
        current.update(data)
        return None

    def get_live_candle(self, token_ca: str, timeframe: Timeframe) -> Optional[LiveCandle]:
        """Get the current in-progress candle (for real-time price checks)."""
        return self._active.get((token_ca, timeframe))

    def remove_token(self, token_ca: str):
        """Remove all candles for a token."""
        keys_to_remove = [k for k in self._active if k[0] == token_ca]
        for k in keys_to_remove:
            del self._active[k]

    def flush_all(self) -> List[Candle]:
        """Finalize and return all active candles (used during shutdown)."""
        candles = []
        for lc in self._active.values():
            if lc.update_count > 0:
                candles.append(lc.to_candle())
        self._active.clear()
        return candles


# ═══════════════════════════════════════════════════════════════
# SUBSCRIPTION MANAGER
# ═══════════════════════════════════════════════════════════════

class SubscriptionManager:
    """
    Manages the set of active token subscriptions.
    
    Handles building complex query strings and tracking which
    tokens need which timeframes.
    
    Confirmed behavior: complex query is a FULL REPLACEMENT.
    Adding/removing a token requires resending the entire subscription.
    """

    def __init__(self):
        # token_ca → set of Timeframes subscribed
        self._subscriptions: Dict[str, Set[Timeframe]] = {}

    @property
    def token_count(self) -> int:
        return len(self._subscriptions)

    @property
    def subscription_count(self) -> int:
        """Total number of (token, timeframe) pairs."""
        return sum(len(tfs) for tfs in self._subscriptions.values())

    def add_token(self, token_ca: str, timeframes: List[Timeframe]):
        """Register a token for subscription. Does NOT send the message."""
        if token_ca not in self._subscriptions:
            self._subscriptions[token_ca] = set()
        self._subscriptions[token_ca].update(timeframes)

    def remove_token(self, token_ca: str):
        """Remove a token. Does NOT send the message."""
        self._subscriptions.pop(token_ca, None)

    def has_token(self, token_ca: str) -> bool:
        return token_ca in self._subscriptions

    def get_all_tokens(self) -> List[str]:
        return list(self._subscriptions.keys())

    def build_subscribe_messages(self) -> List[dict]:
        """
        Build price-subscription message(s) from the provider templates.

        If exactly 1 (token, timeframe) pair: single-subscription message.
        Otherwise: batched query (capped clauses per message); multiple
        messages if the cap is exceeded.
        """
        # Flatten to list of (token_ca, timeframe) pairs
        pairs = []
        for ca, tfs in self._subscriptions.items():
            for tf in tfs:
                pairs.append((ca, tf))

        if not pairs:
            return []

        # Simple case: single subscription
        if len(pairs) == 1:
            ca, tf = pairs[0]
            return [_fill(WS["single_subscribe_template"],
                          address=ca, interval=_timeframe_to_chart_type(tf))]

        # Batched case: capped clauses per message
        messages = []
        for i in range(0, len(pairs), MAX_COMPLEX_ADDRESSES):
            batch = pairs[i:i + MAX_COMPLEX_ADDRESSES]
            clauses = [
                WS["batch_clause_template"].format(
                    address=ca, interval=_timeframe_to_chart_type(tf))
                for ca, tf in batch
            ]
            query = WS["batch_clause_joiner"].join(clauses)
            messages.append(_fill(WS["batch_subscribe_template"], query=query))
        return messages

    @staticmethod
    def build_unsubscribe_message() -> dict:
        """Unsubscribe from all price subscriptions on this connection."""
        return _fill(WS["unsubscribe_template"])


# ═══════════════════════════════════════════════════════════════
# WEBSOCKET CLIENT
# ═══════════════════════════════════════════════════════════════

# Type for the candle callback: called when a candle finalizes
CandleCallback = Callable[[Candle], None]

# Type for the tick callback: called on every price update (V5)
TickCallback = Callable[['TickData'], None]


class DataProviderWebSocket:
    """
    Async WebSocket client for the provider's real-time OHLCV data.
    
    V5: Emits live TickData for real-time entry/exit detection.
    V5.1 fix: Ticks are queued (non-blocking) and consumed by a separate
    async task, preventing back-pressure from starving the WS event loop.
    """

    def __init__(
        self,
        api_key: str,
        on_candle: CandleCallback,
        on_tick: Optional[TickCallback] = None,
        reconnect_delay: float = RECONNECT_BASE_DELAY,
    ):
        self.api_key = api_key
        self.on_candle = on_candle
        self.on_tick = on_tick
        self.reconnect_base_delay = reconnect_delay

        self.subscriptions = SubscriptionManager()
        self.assembler = CandleAssembler()

        # Connection state
        self._ws = None                     # websockets connection object
        self._connected = False
        self._running = False
        self._reconnect_attempts = 0

        # Credit tracking (per byte received)
        self.total_bytes_received = 0
        self.total_credits_consumed = 0.0
        self.total_messages_received = 0

        # V7.1 FIX: track wall-clock of last received message so the
        # heartbeat watchdog can detect a silently-stalled WS feed even
        # if no exception is ever raised. Initialized at construction so
        # a never-connected client doesn't read as infinitely stale.
        self._last_message_monotonic: float = time.monotonic()

        # Market cap is NOT in price events. Populated via the token-stats
        # stream over the same socket (cheaper than per-token REST polling),
        # or REST token overview as fallback.
        self._market_cap_cache: Dict[str, float] = {}

        # ── Tick queue (V5.1 fix) ─────────────────────────────
        # Decouples tick emission (fast, non-blocking put) from
        # tick processing (sync strategy logic + DB writes).
        # The WS message loop stays fast; a separate consumer
        # task drains the queue at its own pace.
        self._tick_queue: Optional[asyncio.Queue] = None
        self._tick_consumer_task: Optional[asyncio.Task] = None
        # Per-token throttle: {token_ca → last_processed_monotonic_time}
        self._tick_last_processed: Dict[str, float] = {}
        self.ticks_queued = 0
        self.ticks_dropped = 0
        self.ticks_processed = 0
        self.ticks_throttled = 0

    @property
    def ws_url(self) -> str:
        return WS["url_template"].format(base=WS_URL, key=self.api_key)

    # ── Connection lifecycle ─────────────────────────────────

    async def connect(self):
        """Establish WebSocket connection."""
        try:
            import websockets
        except ImportError:
            raise ImportError(
                "websockets package required: pip install websockets"
            )

        self._running = True
        logger.info("Connecting to data-provider WebSocket...")

        try:
            self._ws = await websockets.connect(
                self.ws_url,
                subprotocols=[WS_PROTOCOL],
                ping_interval=PING_INTERVAL,
                ping_timeout=PONG_TIMEOUT,
            )
            self._connected = True
            self._reconnect_attempts = 0
            logger.info("WebSocket connected")

            # Re-subscribe if we have tokens (reconnection case)
            if self.subscriptions.token_count > 0:
                await self._send_subscriptions()

        except Exception as e:
            logger.error(f"Connection failed: {e}")
            self._connected = False
            raise

    async def disconnect(self):
        """Clean shutdown."""
        self._running = False
        if self._ws and self._connected:
            try:
                # Unsubscribe first
                msg = SubscriptionManager.build_unsubscribe_message()
                await self._ws.send(json.dumps(msg))
                await self._ws.close()
            except Exception as e:
                logger.warning(f"Disconnect error: {e}")
            finally:
                self._connected = False
                self._ws = None

        # Flush remaining candles
        remaining = self.assembler.flush_all()
        for candle in remaining:
            self.on_candle(candle)

        logger.info(
            f"WebSocket disconnected. "
            f"Total: {self.total_messages_received} msgs, "
            f"{self.total_credits_consumed:.1f} credits"
        )

    # ── Message loop ─────────────────────────────────────────

    async def listen(self):
        """
        Main message loop. Runs until disconnect() is called.
        Handles reconnection automatically.

        V7.1 FIX: Previously used `async for raw in self._ws`, which could
        hang indefinitely inside the websockets library's close_exc path
        when a connection half-died (TCP gone but library state stuck),
        freezing the entire asyncio event loop (incident 2026-05-27).

        Now each message is awaited with asyncio.wait_for(RECV_TIMEOUT).
        A timeout is NOT necessarily an error — an idle feed is normal —
        but it returns control to the loop so we can (a) update the
        watchdog clock and (b) re-check self._running. If the connection
        is genuinely dead, the next recv() raises and we reconnect.
        """
        while self._running:
            try:
                if not self._connected:
                    await self.connect()

                while self._running:
                    try:
                        raw_message = await asyncio.wait_for(
                            self._ws.recv(), timeout=RECV_TIMEOUT
                        )
                    except asyncio.TimeoutError:
                        # No message within RECV_TIMEOUT. This can be a
                        # legitimately quiet period. Probe the connection
                        # with a ping; if the socket is dead this raises
                        # and drops us into reconnect. If it's alive, we
                        # simply continue waiting.
                        logger.warning(
                            f"WS recv timeout ({RECV_TIMEOUT:.0f}s no message) "
                            f"— probing connection with ping"
                        )
                        try:
                            pong_waiter = await self._ws.ping()
                            await asyncio.wait_for(pong_waiter, timeout=PONG_TIMEOUT)
                            # Pong received: connection alive, keep waiting.
                            continue
                        except Exception as ping_err:
                            logger.warning(
                                f"WS ping failed after recv timeout: {ping_err} "
                                f"— forcing reconnect"
                            )
                            raise  # propagate to outer reconnect handler

                    if not self._running:
                        break
                    self._process_raw_message(raw_message)

            except Exception as e:
                if not self._running:
                    break
                logger.warning(f"WebSocket error: {e}")
                self._connected = False
                # Defensive: drop any stale connection object so connect()
                # always builds a fresh one on the next loop iteration.
                self._ws = None
                await self._reconnect()

    async def _reconnect(self):
        """Reconnect with exponential backoff."""
        self._reconnect_attempts += 1
        delay = min(
            self.reconnect_base_delay * (2 ** (self._reconnect_attempts - 1)),
            RECONNECT_MAX_DELAY,
        )
        logger.info(
            f"Reconnecting in {delay:.1f}s "
            f"(attempt {self._reconnect_attempts})"
        )
        await asyncio.sleep(delay)

        try:
            await self.connect()
        except Exception:
            pass  # Will retry in the listen loop

    # ── Tick queue consumer (V5.1 fix) ───────────────────────

    def start_tick_consumer(self):
        """
        Initialize the tick queue and start the consumer task.
        Must be called after the event loop is running (from an async context).
        
        The consumer pulls ticks from the queue and dispatches them to
        on_tick() in a separate async task, so the WS message loop is
        never blocked by strategy processing or DB writes.
        """
        self._tick_queue = asyncio.Queue(maxsize=TICK_QUEUE_MAX_SIZE)
        self._tick_consumer_task = asyncio.create_task(
            self._tick_consumer_loop()
        )
        logger.info(
            f"Tick consumer started (queue_size={TICK_QUEUE_MAX_SIZE}, "
            f"throttle={TICK_THROTTLE_SECONDS}s)"
        )

    async def stop_tick_consumer(self):
        """Stop the tick consumer task. Call before disconnect."""
        if self._tick_consumer_task and not self._tick_consumer_task.done():
            self._tick_consumer_task.cancel()
            try:
                await self._tick_consumer_task
            except asyncio.CancelledError:
                pass
        self._tick_queue = None
        logger.info(
            f"Tick consumer stopped — "
            f"queued={self.ticks_queued} processed={self.ticks_processed} "
            f"throttled={self.ticks_throttled} dropped={self.ticks_dropped}"
        )

    async def _tick_consumer_loop(self):
        """
        Async consumer loop: pulls ticks from the queue and dispatches
        to on_tick(). Runs as a separate task alongside listen().
        
        Throttle: skips ticks for a token if one was processed less than
        TICK_THROTTLE_SECONDS ago. This ensures we don't waste CPU on
        redundant price checks for the same token at the same indicator levels.
        """
        while self._running:
            try:
                # Block until a tick is available (with timeout to check _running)
                try:
                    tick = await asyncio.wait_for(
                        self._tick_queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                # Per-token throttle
                now_mono = time.monotonic()
                last = self._tick_last_processed.get(tick.token_ca, 0.0)
                if (now_mono - last) < TICK_THROTTLE_SECONDS:
                    self.ticks_throttled += 1
                    continue

                self._tick_last_processed[tick.token_ca] = now_mono

                # Dispatch to the callback (this is the sync strategy processing)
                # It runs here in the consumer task, NOT in the WS message loop.
                if self.on_tick is not None:
                    self.on_tick(tick)
                self.ticks_processed += 1

                # Yield control to the event loop after each tick processed.
                # This ensures the WS listen loop can read messages, send pongs, etc.
                await asyncio.sleep(0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Tick consumer error: {e}", exc_info=True)
                await asyncio.sleep(0.1)  # Prevent tight error loop

    # ── Message processing ───────────────────────────────────

    def _process_raw_message(self, raw: str):
        """Parse and route a raw WebSocket message."""
        msg_bytes = len(raw.encode('utf-8'))
        self.total_bytes_received += msg_bytes
        self.total_credits_consumed += msg_bytes * CREDIT_PER_BYTE
        self.total_messages_received += 1
        self._last_message_monotonic = time.monotonic()  # V7.1 watchdog clock

        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON: {raw[:100]}")
            return

        msg_type = msg.get(WS["type_key"], "")

        if msg_type == WS["price_event"]:
            self._handle_price_data(msg.get("data", {}))
        elif msg_type == WS["stats_event"]:
            self._handle_token_stats(msg.get("data", {}))
        else:
            logger.debug(f"Unhandled message type: {msg_type}")

    def _handle_price_data(self, data: dict):
        """
        Process a streaming price update.
        
        V5.1 fix: Tick is queued (non-blocking) instead of calling on_tick
        synchronously. The tick consumer task processes queued ticks
        without blocking the WebSocket message loop.
        
        Candle assembly remains synchronous (fast, no external calls).
        """
        address = data.get(FIELDS["event_address"], "")
        chart_type = data.get(FIELDS["event_interval"], "")

        if not address or not chart_type:
            return

        timeframe = _chart_type_to_timeframe(chart_type)

        # Inject market cap from cache
        mcap = self._market_cap_cache.get(address, 0.0)
        if address in self._market_cap_cache:
            data["market_cap"] = mcap

        # Queue tick for async processing (non-blocking)
        if self._tick_queue is not None and self.on_tick is not None:
            close_price = data.get(FIELDS["candle_close"], 0)
            volume_base = data.get(FIELDS["candle_volume"], 0)
            if close_price and close_price > 0:
                tick = TickData(
                    token_ca=address,
                    price=close_price,
                    market_cap=mcap,
                    timestamp=datetime.now(timezone.utc),
                    timeframe=timeframe,
                    volume_usd=volume_base * close_price if volume_base else 0,
                )
                try:
                    self._tick_queue.put_nowait(tick)
                    self.ticks_queued += 1
                except asyncio.QueueFull:
                    # Queue full — drop oldest, put new
                    try:
                        self._tick_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                    try:
                        self._tick_queue.put_nowait(tick)
                        self.ticks_queued += 1
                    except asyncio.QueueFull:
                        pass
                    self.ticks_dropped += 1

        # Candle assembly (fast, in-memory — stays synchronous)
        completed_candle = self.assembler.process_update(
            address, timeframe, data
        )

        if completed_candle is not None:
            # Set market cap from token-stats cache
            if completed_candle.market_cap == 0 and address in self._market_cap_cache:
                completed_candle.market_cap = self._market_cap_cache[address]

            self.on_candle(completed_candle)

    # ── Token stats handling (market-cap stream) ──────────────

    def _handle_token_stats(self, data: dict):
        """
        Process a token-stats update: refreshes the market-cap cache.
        Streaming stats over the existing socket is much cheaper than
        per-token REST polling.
        """
        address = data.get(FIELDS["event_address"], "")
        if not address:
            return

        # Try the configured market-cap field names in order
        mcap = None
        for key in FIELDS["stats_market_cap_keys"]:
            mcap = data.get(key)
            if mcap:
                break
        if mcap is not None:
            try:
                self._market_cap_cache[address] = float(mcap)
            except (ValueError, TypeError):
                pass

    # ── Subscription management ──────────────────────────────

    async def subscribe_tokens(
        self, tokens: List[str], timeframes: List[Timeframe],
    ):
        """
        Subscribe to OHLCV data for a list of tokens.
        Rebuilds the entire subscription (complex query = full replacement).
        """
        for ca in tokens:
            self.subscriptions.add_token(ca, timeframes)

        if self._connected:
            await self._send_subscriptions()

    async def add_token(self, token_ca: str, timeframes: List[Timeframe]):
        """Add one token and rebuild subscription."""
        self.subscriptions.add_token(token_ca, timeframes)
        if self._connected:
            await self._send_subscriptions()

    async def remove_token(self, token_ca: str):
        """Remove one token, rebuild subscription, clean up assembler."""
        self.subscriptions.remove_token(token_ca)
        self.assembler.remove_token(token_ca)
        self._market_cap_cache.pop(token_ca, None)

        if self._connected:
            if self.subscriptions.token_count > 0:
                await self._send_subscriptions()
            else:
                # No tokens left — unsubscribe
                msg = SubscriptionManager.build_unsubscribe_message()
                await self._ws.send(json.dumps(msg))

    async def _send_subscriptions(self):
        """Build and send subscription message(s)."""
        messages = self.subscriptions.build_subscribe_messages()
        for msg in messages:
            await self._ws.send(json.dumps(msg))
            n = self.subscriptions.subscription_count
            logger.info(f"Subscription sent: {n} token/timeframe pairs")

    # ── Market cap management (✅ CONFIRMED Q4) ──────────────

    def update_market_cap(self, token_ca: str, market_cap: float):
        """
        Update cached market cap for a token.
        
        Market cap is not in price events.
        Primary source: token-stats stream via _handle_token_stats().
        This method is also available for REST fallback if needed.
        """
        self._market_cap_cache[token_ca] = market_cap

    # ── Stats ────────────────────────────────────────────────

    def seconds_since_last_message(self) -> float:
        """V7.1: wall-clock seconds since the last WS message was received.
        Used by the heartbeat watchdog to detect a silently-stalled feed."""
        return time.monotonic() - self._last_message_monotonic

    async def force_reconnect(self):
        """V7.1: externally-triggered reconnect, called by the heartbeat
        watchdog when the feed has gone silent past threshold but the
        listen loop hasn't yet noticed (defense-in-depth). Safe to call
        from another task: it just closes the socket, which makes the
        in-flight recv() raise and the listen loop reconnect on its own."""
        logger.warning("force_reconnect() invoked by watchdog")
        self._connected = False
        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                await ws.close()
            except Exception as e:
                logger.warning(f"force_reconnect close error (ignored): {e}")

    def get_stats(self) -> dict:
        return {
            "connected": self._connected,
            "tokens_subscribed": self.subscriptions.token_count,
            "subscriptions": self.subscriptions.subscription_count,
            "total_messages": self.total_messages_received,
            "total_bytes": self.total_bytes_received,
            "total_credits": round(self.total_credits_consumed, 2),
            "reconnect_attempts": self._reconnect_attempts,
            "seconds_since_last_message": round(self.seconds_since_last_message(), 1),
            "ticks_queued": self.ticks_queued,
            "ticks_processed": self.ticks_processed,
            "ticks_throttled": self.ticks_throttled,
            "ticks_dropped": self.ticks_dropped,
            "tick_queue_size": self._tick_queue.qsize() if self._tick_queue else 0,
        }
