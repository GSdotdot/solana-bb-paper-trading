"""
Funnel Manager — core lifecycle engine for the scanner.

Token-list discovery flow (three-stage funnel: WIDE → NARROW → FULL FOCUS):
  1. Token-list poll returns server-side pre-filtered tokens
  2. PreFilterGate: PumpFun source check, blacklist, duplicate
  3. Wide evaluation (zero extra API credits — uses token-list data only):
     - Meets all strategy thresholds → straight to Narrow
     - Close but not quite → park in Wide, re-evaluate on next list poll
     - Fails fee check → permanent blacklist, dropped
  4. Narrow scan (ONE paid security call per token — holder concentration):
     - Pass → promote to Full
     - Near-fail (within tolerance) → demote to Wide
     - Hard fail → permanent blacklist, dropped
  5. Full Focus: WebSocket OHLCV subscription for strategies (only survivors)

Polling cadence adapts to the funnel's PHASE (see get_recommended_intervals):
  INITIAL_FILL → poll aggressively until the Full bucket is first filled
  MAINTENANCE  → back off in steady state to conserve API credits
  REFILL       → speed up again when a bucket is depleted / critical

Does NOT make API calls — pure logic, fully testable with mock data.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Dict, List, Optional, Tuple

from paper_trading.config.params import param
from paper_trading.scanner.token_model import ScannerToken, Tier, Launchpad
from paper_trading.scanner.buckets import BucketSet
from paper_trading.scanner.blacklist import BlacklistManager
from paper_trading.scanner.filters import (
    PreFilterGate, WideScanFilters, NarrowScanFilters, FilterResult,
)

logger = logging.getLogger(__name__)


class Phase(Enum):
    INITIAL_FILL = "initial_fill"
    MAINTENANCE = "maintenance"
    REFILL = "refill"


FullPromotionCallback = Callable[[str, str], None]
FullRemovalCallback = Callable[[str, str], None]


@dataclass
class FunnelEvent:
    timestamp: datetime
    event_type: str
    token_ca: str
    ticker: str
    from_tier: str
    to_tier: str
    reason: str = ""


NarrowPromotionCallback = Callable[[str, str], None]  # (token_ca, ticker)

class FunnelManager:
    def __init__(
        self,
        buckets: Optional[BucketSet] = None,
        blacklist: Optional[BlacklistManager] = None,
        pre_filter: Optional[PreFilterGate] = None,
        wide_filters: Optional[WideScanFilters] = None,
        narrow_filters: Optional[NarrowScanFilters] = None,
        on_full_promotion: Optional[FullPromotionCallback] = None,
        on_full_removal: Optional[FullRemovalCallback] = None,
        on_narrow_promotion: Optional[NarrowPromotionCallback] = None,
    ):
        self.buckets = buckets or BucketSet()
        self.blacklist = blacklist or BlacklistManager()
        self.pre_filter = pre_filter or PreFilterGate()
        self.wide_filters = wide_filters or WideScanFilters()
        self.narrow_filters = narrow_filters or NarrowScanFilters()
        self._on_full_promotion = on_full_promotion
        self._on_full_removal = on_full_removal
        self._on_narrow_promotion = on_narrow_promotion
        self._events: List[FunnelEvent] = []
        self._phase = Phase.INITIAL_FILL
        self.total_discovered = 0
        self.total_rejected_prefilter = 0
        self.total_rejected_wide = 0
        self.total_rejected_narrow = 0
        self.total_promoted_to_narrow = 0
        self.total_promoted_to_full = 0
        self.total_parked_in_wide = 0
        self.total_blacklisted = 0
        self.total_expired = 0

    @property
    def phase(self) -> Phase:
        return self._phase

    def update_phase(self) -> Phase:
        if (self._phase == Phase.INITIAL_FILL
                and self.total_promoted_to_full < self.buckets.full.capacity):
            return self._phase
        if (self.buckets.wide.is_depleted
                or self.buckets.narrow.is_depleted
                or self.buckets.full.is_depleted):
            self._phase = Phase.REFILL
        else:
            self._phase = Phase.MAINTENANCE
        return self._phase

    # ════════════════════════════════════════════════════════════
    # TOKEN-LIST PROCESSING — main entry point for new discoveries
    # ════════════════════════════════════════════════════════════

    def process_list_token(self, token: ScannerToken) -> FilterResult:
        """
        Process a token from token list response.
        This is the main entry point replacing the old admit_candidate flow.

        Flow:
          1. Pre-filter (PumpFun, blacklist, duplicate)
          2. Wide evaluation (mcap, liquidity ratio, fee generation)
          3. Route: straight to Narrow, park in Wide, or reject

        Returns FilterResult with routing decision.
        """
        self.total_discovered += 1

        # Check blacklist first (includes permanent bans from fee failures)
        if self.blacklist.is_blacklisted(token.token_ca):
            self.total_rejected_prefilter += 1
            return FilterResult(passed=False, token_ca=token.token_ca,
                               failed_filter="blacklist", reason="blacklisted")

        # Already in funnel? Update token-list data for tokens in Wide (re-evaluation)
        existing = self.buckets.find_token(token.token_ca)
        if existing is not None:
            bucket, existing_token = existing
            if bucket.name == "wide":
                # Update stored token with fresh token-list data before re-evaluating
                existing_token.market_cap = token.market_cap or existing_token.market_cap
                existing_token.liquidity = token.liquidity or existing_token.liquidity
                existing_token.volume_24h_usd = token.volume_24h_usd or existing_token.volume_24h_usd
                existing_token.volume_5m_usd = token.volume_5m_usd or existing_token.volume_5m_usd
                existing_token.volume_1h_usd = token.volume_1h_usd or existing_token.volume_1h_usd
                existing_token.price = token.price or existing_token.price
                existing_token.holder_count = token.holder_count or existing_token.holder_count
                existing_token.list_appearances += 1
                return self._reevaluate_wide_token(existing_token)
            # In Narrow or Full — just note the list appearance
            existing_token.list_appearances += 1
            return FilterResult(passed=True, token_ca=token.token_ca,
                               failed_filter="", reason="already in funnel")

        # Pre-filter gate (PumpFun check, duplicate check)
        self.pre_filter._in_funnel = set(
            self.buckets.wide.get_all_cas()
            + self.buckets.narrow.get_all_cas()
            + self.buckets.full.get_all_cas()
        )
        pf_result = self.pre_filter.run(token)
        if not pf_result.passed:
            self.total_rejected_prefilter += 1
            self._log_event("reject", token, "candidate", "rejected", pf_result.reason)
            return pf_result

        # Wide evaluation (zero extra credits — all from token-list data)
        return self._evaluate_and_route(token)

    def _evaluate_and_route(self, token: ScannerToken) -> FilterResult:
        """
        Run Wide scan filters and route token based on results.
        Tokens meeting all thresholds go straight to Narrow.
        Borderline tokens park in Wide for re-evaluation.
        Fee check failures get permanently blacklisted.
        """
        token.wide_scan_count += 1
        token.wide_eval_done = True
        result = self.wide_filters.run(token)

        if result.passed:
            # All thresholds met → straight to Narrow
            return self._admit_to_narrow(token)

        if result.permanent_blacklist:
            # Fee check failure → permanent blacklist, non-negotiable
            self.blacklist.add_permanent(token.token_ca, result.reason)
            self.pre_filter.blacklist_token(token.token_ca)
            self.total_blacklisted += 1
            self.total_rejected_wide += 1
            self._log_event("blacklist", token, "candidate", "blacklisted", result.reason)
            return result

        # Borderline failure (mcap or liquidity close but not there) → park in Wide
        if self._is_borderline(token, result):
            return self._park_in_wide(token, result.reason)

        # Hard fail on mcap/liquidity → just reject, no blacklist
        self.total_rejected_wide += 1
        self._log_event("reject", token, "candidate", "rejected", result.reason)
        return result

    def _is_borderline(self, token: ScannerToken, result: FilterResult) -> bool:
        """
        Check if a token is close to qualifying (within tolerance).
        Borderline = within a tolerance factor of the mcap or liquidity-ratio
        threshold (factors: XXXX, see scanner.borderline in config).
        """
        lo = param("scanner.borderline.below_min_factor")
        hi = param("scanner.borderline.above_max_factor")
        if result.failed_filter == "mcap_range" and token.market_cap is not None:
            # Close to min mcap?
            if token.market_cap >= self.wide_filters.min_mcap * lo:
                return True
            # Slightly over max? Less useful but possible
            if token.market_cap <= self.wide_filters.max_mcap * hi:
                return True
        if result.failed_filter == "liquidity_ratio" and token.liquidity_ratio is not None:
            if token.liquidity_ratio >= self.wide_filters.min_liquidity_ratio * lo:
                return True
        return False

    def _park_in_wide(self, token: ScannerToken, reason: str) -> FilterResult:
        """Park a borderline token in Wide for re-evaluation on next list poll."""
        token.promote_to(Tier.WIDE)
        if self.buckets.wide.add(token):
            self.pre_filter.mark_in_funnel(token.token_ca)
            self.total_parked_in_wide += 1
            self._log_event("park", token, "candidate", "wide", f"borderline: {reason}")
            return FilterResult(passed=True, token_ca=token.token_ca,
                               failed_filter="borderline",
                               reason=f"parked in wide: {reason}")
        # Wide bucket full — try evicting oldest
        if self.buckets.wide.is_full:
            evicted = self.buckets.wide.evict_oldest()
            if evicted:
                self.pre_filter.unmark_in_funnel(evicted.token_ca)
                evicted.expel("evicted_by_newer")
                self._log_event("expire", evicted, "wide", "expelled",
                               "evicted by newer candidate")
            if self.buckets.wide.add(token):
                self.pre_filter.mark_in_funnel(token.token_ca)
                self.total_parked_in_wide += 1
                self._log_event("park", token, "candidate", "wide",
                               f"borderline (replaced evicted): {reason}")
                return FilterResult(passed=True, token_ca=token.token_ca,
                                   failed_filter="borderline",
                                   reason=f"parked in wide: {reason}")
        self.total_rejected_wide += 1
        return FilterResult(passed=False, token_ca=token.token_ca,
                           failed_filter="capacity", reason="wide bucket full")

    def _admit_to_narrow(self, token: ScannerToken) -> FilterResult:
        """Promote token directly to Narrow scan."""
        if self.buckets.narrow.available_slots > 0:
            token.promote_to(Tier.NARROW)
            self.buckets.narrow.add(token)
            self.pre_filter.mark_in_funnel(token.token_ca)
            self.total_promoted_to_narrow += 1
            self._log_event("promote", token, "candidate", "narrow", "passed all wide checks")
            if self._on_narrow_promotion:
                self._on_narrow_promotion(token.token_ca, token.ticker)
            return FilterResult(passed=True, token_ca=token.token_ca)
        # Narrow full — park in Wide instead
        return self._park_in_wide(token, "passed wide filters but narrow bucket full")

    def _reevaluate_wide_token(self, token: ScannerToken) -> FilterResult:
        """
        Re-evaluate a token already parked in Wide.
        Called when the token reappears in list poll with potentially updated data.
        """
        token.wide_scan_count += 1
        result = self.wide_filters.run(token)

        if result.passed:
            # Now qualifies! Promote to Narrow
            self.buckets.wide.remove(token.token_ca)
            if self.buckets.narrow.available_slots > 0:
                token.promote_to(Tier.NARROW)
                self.buckets.narrow.add(token)
                self.total_promoted_to_narrow += 1
                self._log_event("promote", token, "wide", "narrow",
                               "re-evaluation passed")
                if self._on_narrow_promotion:
                    self._on_narrow_promotion(token.token_ca, token.ticker)
                return FilterResult(passed=True, token_ca=token.token_ca)
            else:
                # Narrow full — keep in Wide
                self.buckets.wide.add(token)
                return FilterResult(passed=True, token_ca=token.token_ca,
                                   failed_filter="narrow_full",
                                   reason="passed re-eval but narrow full")

        if result.permanent_blacklist:
            # Fee check failed on re-eval — permanent blacklist
            self.buckets.wide.remove(token.token_ca)
            self.pre_filter.unmark_in_funnel(token.token_ca)
            self.blacklist.add_permanent(token.token_ca, result.reason)
            self.pre_filter.blacklist_token(token.token_ca)
            self.total_blacklisted += 1
            self._log_event("blacklist", token, "wide", "blacklisted", result.reason)
            return result

        # Still borderline — stays in Wide, will TTL out eventually
        return FilterResult(passed=False, token_ca=token.token_ca,
                           failed_filter=result.failed_filter,
                           reason=f"still borderline: {result.reason}")

    # ════════════════════════════════════════════════════════════
    # NARROW SCAN EVALUATION
    # ════════════════════════════════════════════════════════════

    def evaluate_narrow_token(self, token_ca: str) -> FilterResult:
        """
        Run Narrow scan filters (holder concentration).
        Called after the token-security (holder) data has been fetched.
        """
        token = self.buckets.narrow.get(token_ca)
        if token is None:
            return FilterResult(passed=False, token_ca=token_ca,
                               failed_filter="not_found", reason="not in narrow")

        token.narrow_scan_count += 1
        token.narrow_scan_done = True
        result = self.narrow_filters.run(token)

        if result.passed:
            # Promote to Full
            if self.buckets.full.available_slots > 0:
                self.buckets.narrow.remove(token_ca)
                token.promote_to(Tier.FULL)
                self.buckets.full.add(token)
                self.total_promoted_to_full += 1
                self._log_event("promote", token, "narrow", "full")
                if self._on_full_promotion:
                    self._on_full_promotion(token.token_ca, token.ticker)
                return FilterResult(passed=True, token_ca=token_ca)
            return FilterResult(passed=True, token_ca=token_ca,
                               failed_filter="full_full",
                               reason="passed filters but full bucket at capacity")

        # Failed — check if near-fail (within tolerance)
        if self.narrow_filters.is_near_fail(token):
            # Demote back to Wide for re-evaluation
            self.buckets.narrow.remove(token_ca)
            token.promote_to(Tier.WIDE)
            token.narrow_scan_done = False  # Reset for potential future re-scan
            self.buckets.wide.add(token)
            self._log_event("demote", token, "narrow", "wide",
                           f"near-fail tolerance: {result.reason}")
            return result

        # Hard fail — permanent blacklist
        self.buckets.narrow.remove(token_ca)
        self.pre_filter.unmark_in_funnel(token_ca)
        self.blacklist.add_permanent(token_ca, result.reason)
        self.pre_filter.blacklist_token(token_ca)
        self.total_rejected_narrow += 1
        self.total_blacklisted += 1
        self._log_event("blacklist", token, "narrow", "blacklisted", result.reason)
        result.permanent_blacklist = True
        return result

    # ════════════════════════════════════════════════════════════
    # FULL FOCUS MANAGEMENT
    # ════════════════════════════════════════════════════════════

    def remove_from_full(self, token_ca: str, reason: str = "traded"):
        """Remove a token from Full focus. Cooldown blacklist (not permanent)."""
        token = self.buckets.full.remove(token_ca)
        if token is None:
            return
        token.expel(reason)
        self.pre_filter.unmark_in_funnel(token_ca)
        self.blacklist.add_cooldown(token_ca, reason)
        self._log_event("expire", token, "full", "blacklisted", reason)
        if self._on_full_removal:
            self._on_full_removal(token_ca, reason)

    def remove_token(self, token_ca: str, reason: str = "manual"):
        """Remove a token from any tier. No blacklist (silent drop)."""
        result = self.buckets.find_token(token_ca)
        if result is None:
            return
        bucket, token = result
        bucket.remove(token_ca)
        token.expel(reason)
        self.pre_filter.unmark_in_funnel(token_ca)
        self._log_event("expire", token, bucket.name, "expelled", reason)

    # ════════════════════════════════════════════════════════════
    # TTL & MAINTENANCE
    # ════════════════════════════════════════════════════════════

    def enforce_ttls(self) -> int:
        """
        Enforce TTL on all buckets.
        Wide/Narrow: silent drop, no blacklist.
        Full: notifies orchestrator via on_full_removal callback.
        """
        expired_wide = self.buckets.wide.enforce_ttl()
        expired_narrow = self.buckets.narrow.enforce_ttl()
        expired_full = self.buckets.full.enforce_ttl()

        for token in expired_wide + expired_narrow:
            token.expel("ttl_expired")
            self.pre_filter.unmark_in_funnel(token.token_ca)
            self._log_event("expire", token, token.tier.value, "expelled", "ttl_expired")

        for token in expired_full:
            token.expel("ttl_expired")
            self.pre_filter.unmark_in_funnel(token.token_ca)
            self._log_event("expire", token, "full", "expelled", "ttl_expired")
            if self._on_full_removal:
                self._on_full_removal(token.token_ca, "ttl_expired")

        total = len(expired_wide) + len(expired_narrow) + len(expired_full)
        self.total_expired += total
        return total

    # ════════════════════════════════════════════════════════════
    # QUERY HELPERS
    # ════════════════════════════════════════════════════════════

    def get_narrow_tokens_needing_scan(self) -> List[str]:
        """Get tokens in Narrow that haven't been safety-checked yet."""
        return [ca for ca in self.buckets.narrow.get_all_cas()
                if not self.buckets.narrow.get(ca).narrow_scan_done]

    def get_wide_tokens_for_scan(self) -> List[str]:
        return self.buckets.wide.get_all_cas()

    def get_narrow_tokens_for_scan(self) -> List[str]:
        return self.buckets.narrow.get_all_cas()

    def get_full_tokens(self) -> List[str]:
        return self.buckets.full.get_all_cas()

    def get_recommended_intervals(self) -> dict:
        """
        Phase-dependent polling cadence — the main lever on API credit spend.
        Each call to the token list and each narrow security check costs
        credits, so the funnel polls aggressively only while it is filling its
        pipeline and backs off once it is in steady state. All minute values
        are tuned privately (config → funnels.phase_intervals).
        """
        self.update_phase()
        pi = "funnels.phase_intervals"
        if self._phase == Phase.INITIAL_FILL:
            return dict(param(f"{pi}.initial_fill"))
        if self._phase == Phase.MAINTENANCE:
            # Steady state: slower cadence. The trade-off is the "miss window"
            # for newly eligible tokens vs. monthly credit consumption.
            return dict(param(f"{pi}.maintenance"))
        # Refill: faster narrow scans when a downstream bucket is critical
        narrow_critical = self.buckets.narrow.is_critical
        full_critical = self.buckets.full.is_critical
        refill = param(f"{pi}.refill")
        return {
            "list_poll_minutes": refill["list_poll_minutes"],
            "narrow_scan_minutes": (refill["narrow_scan_minutes_critical"]
                                    if (narrow_critical or full_critical)
                                    else refill["narrow_scan_minutes"]),
            "ttl_check_minutes": refill["ttl_check_minutes"],
        }

    # ════════════════════════════════════════════════════════════
    # LOGGING & STATUS
    # ════════════════════════════════════════════════════════════

    def _log_event(self, event_type, token, from_tier, to_tier, reason=""):
        event = FunnelEvent(
            timestamp=datetime.now(timezone.utc), event_type=event_type,
            token_ca=token.token_ca, ticker=token.ticker,
            from_tier=from_tier, to_tier=to_tier, reason=reason,
        )
        self._events.append(event)

    @property
    def event_count(self) -> int:
        return len(self._events)

    def get_recent_events(self, n: int = 20) -> List[FunnelEvent]:
        return self._events[-n:]

    def status(self) -> dict:
        self.update_phase()
        return {
            "phase": self._phase.value,
            "buckets": self.buckets.status(),
            "blacklist": self.blacklist.status(),
            "intervals": self.get_recommended_intervals(),
            "counters": {
                "total_discovered": self.total_discovered,
                "rejected_prefilter": self.total_rejected_prefilter,
                "rejected_wide": self.total_rejected_wide,
                "rejected_narrow": self.total_rejected_narrow,
                "promoted_to_narrow": self.total_promoted_to_narrow,
                "promoted_to_full": self.total_promoted_to_full,
                "parked_in_wide": self.total_parked_in_wide,
                "total_blacklisted": self.total_blacklisted,
                "total_expired": self.total_expired,
            },
            "events": self.event_count,
        }
