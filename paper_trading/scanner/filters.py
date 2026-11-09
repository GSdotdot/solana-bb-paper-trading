"""
Filter Chain
=============
Pluggable filter system for each tier of the funnel.
Each filter: (ScannerToken) → (bool, reason_str)
Chains short-circuit on first failure.

Token-list discovery means:
  - PreFilterGate: launchpad/source check, blacklist, duplicate
  - WideScanFilters: token age, mcap range, short-window volume,
    liquidity/mcap ratio, fee-generation check
    (all computed from token-list data — zero additional API credit cost)
  - NarrowScanFilters: holder concentration via the provider's token-security
    endpoint (one paid call per token).
    Mint/freeze authority skipped for PumpFun (revoked by design).

Fee generation check uses 24h volume × DEX fee rate as a proxy for accumulated
fees. Tokens failing the fee check are PERMANENTLY blacklisted.

All thresholds are loaded from config (see config.example.yaml).
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, List, Optional, Set, Tuple

from paper_trading.config.params import param
from paper_trading.scanner.token_model import ScannerToken, Launchpad

logger = logging.getLogger(__name__)

FilterFunc = Callable[[ScannerToken], Tuple[bool, str]]


@dataclass
class FilterResult:
    passed: bool
    token_ca: str
    failed_filter: str = ""
    reason: str = ""
    permanent_blacklist: bool = False    # If True, token must be permanently blacklisted


class FilterChain:
    def __init__(self, name: str):
        self.name = name
        self._filters: List[Tuple[str, FilterFunc]] = []

    def add(self, filter_name: str, filter_func: FilterFunc):
        self._filters.append((filter_name, filter_func))

    def run(self, token: ScannerToken) -> FilterResult:
        for fname, ffunc in self._filters:
            passed, reason = ffunc(token)
            if not passed:
                return FilterResult(passed=False, token_ca=token.token_ca,
                                   failed_filter=fname, reason=reason)
        return FilterResult(passed=True, token_ca=token.token_ca)

    @property
    def filter_count(self) -> int:
        return len(self._filters)


# ═══════════════════════════════════════════════════════════════
# PRE-FILTER GATE
# ═══════════════════════════════════════════════════════════════

class PreFilterGate:
    """
    Zero-cost in-memory checks before a token enters the funnel.

    Source filtering: the token-list response carries a launchpad/source field.
    For paper trading: only PumpFun tokens are accepted.

    Falls back to the PumpFun address-suffix convention if the source field
    is not populated (e.g., manual injection or test data).
    """

    ALLOWED_SOURCES = {param("data_provider.launchpad_source_id")}

    def __init__(self, pumpfun_only: bool = True):
        self.chain = FilterChain("pre_filter")
        self._blacklisted: Set[str] = set()
        self._in_funnel: Set[str] = set()
        self.pumpfun_only = pumpfun_only

        self.chain.add("pumpfun_check", self._check_pumpfun)
        self.chain.add("blacklist_check", self._check_blacklist)
        self.chain.add("duplicate_check", self._check_duplicate)

    def run(self, token: ScannerToken) -> FilterResult:
        return self.chain.run(token)

    def _check_pumpfun(self, token: ScannerToken) -> Tuple[bool, str]:
        """
        Check token source. Preferred: source field from the API.
        Fallback: address suffix check for backward compat.
        """
        if not self.pumpfun_only:
            return (True, "")

        # Primary: API-provided source field
        if token.launchpad_source:
            if token.launchpad_source in self.ALLOWED_SOURCES:
                token.launchpad = Launchpad.PUMPFUN
                return (True, "")
            return (False, f"source '{token.launchpad_source}' not in allowed sources")

        # Fallback: address suffix (for manual injection / test data)
        if token.token_ca.endswith("pump"):
            token.launchpad = Launchpad.PUMPFUN
            return (True, "")
        return (False, f"no launchpad_source and address does not end in 'pump'")

    def _check_blacklist(self, token: ScannerToken) -> Tuple[bool, str]:
        if token.token_ca in self._blacklisted:
            return (False, "blacklisted")
        return (True, "")

    def _check_duplicate(self, token: ScannerToken) -> Tuple[bool, str]:
        if token.token_ca in self._in_funnel:
            return (False, "already in funnel")
        return (True, "")

    def blacklist_token(self, token_ca: str):
        self._blacklisted.add(token_ca)

    def unblacklist_token(self, token_ca: str):
        self._blacklisted.discard(token_ca)

    def mark_in_funnel(self, token_ca: str):
        self._in_funnel.add(token_ca)

    def unmark_in_funnel(self, token_ca: str):
        self._in_funnel.discard(token_ca)

    @property
    def blacklist_size(self) -> int:
        return len(self._blacklisted)


# ═══════════════════════════════════════════════════════════════
# WIDE SCAN FILTERS
# ═══════════════════════════════════════════════════════════════

# Fee table: minimum SOL in accumulated fees required per mcap tier.
# List of [mcap_ceiling_usd, min_fees_sol] pairs, ascending by ceiling.
FEE_TABLE_SOL = param("scanner.wide.fee_table_sol")          # tuned privately
FEE_TABLE_FALLBACK_SOL = param("scanner.wide.fee_table_fallback_sol")
DEX_FEE_PROXY_PCT = param("friction.dex_fee_pct")             # fee-rate proxy


def get_min_fees_sol(market_cap: float) -> float:
    """Look up minimum required fees (in SOL) for a given market cap."""
    for mcap_ceiling, fees_sol in FEE_TABLE_SOL:
        if market_cap <= mcap_ceiling:
            return fees_sol
    return FEE_TABLE_FALLBACK_SOL


class WideScanFilters:
    """
    All checks use data already present in the token-list response —
    zero additional API credit cost.

    Checks (thresholds redacted, see config.example.yaml → scanner.wide):
      1. Token age             → passes if age <= max_age_seconds
                                 (FAIL = permanent blacklist)
      2. Market cap            → passes if mcap within [XXXX, XXXX]
      3. Short-window volume   → passes if volume_5m >= XXXX
      4. Liquidity / mcap      → passes if ratio >= XXXX
      5. Fee generation        → passes if est. fees >= tier minimum
                                 (FAIL = permanent blacklist)

    The fee check requires a current SOL price, injected via set_sol_price()
    and refreshed periodically by a background task.
    """

    def __init__(
        self,
        min_mcap: float = param("scanner.wide.min_mcap"),
        max_mcap: float = param("scanner.wide.max_mcap"),
        min_liquidity_ratio: float = param("scanner.wide.min_liquidity_ratio"),
        sol_price_usd: float = 0.0,        # Unknown until first price poll
        max_age_seconds: int = param("scanner.wide.max_age_seconds"),
        min_volume_5m_usd: float = param("scanner.wide.min_volume_5m_usd"),
    ):
        self.chain = FilterChain("wide_scan")
        self.min_mcap = min_mcap
        self.max_mcap = max_mcap
        self.min_liquidity_ratio = min_liquidity_ratio
        self.sol_price_usd = sol_price_usd
        self.max_age_seconds = max_age_seconds
        self.min_volume_5m_usd = min_volume_5m_usd

        self.chain.add("token_age", self._check_age)
        self.chain.add("mcap_range", self._check_mcap)
        self.chain.add("volume_5m", self._check_volume_5m)
        self.chain.add("liquidity_ratio", self._check_liquidity)
        self.chain.add("fee_generation", self._check_fees)

    def run(self, token: ScannerToken) -> FilterResult:
        result = self.chain.run(token)
        # Mark permanent blacklist if fee check or age check failed
        if not result.passed and result.failed_filter in ("fee_generation", "token_age"):
            result.permanent_blacklist = True
        return result

    def set_sol_price(self, price_usd: float):
        """Update SOL price for fee calculations."""
        self.sol_price_usd = price_usd

    def _check_age(self, token: ScannerToken) -> Tuple[bool, str]:
        """
        Reject tokens older than max_age_seconds. Uses creation_time
        (preferred) or recent_listing_time as fallback.
        Tokens failing this are PERMANENTLY blacklisted — they'll never
        become young enough to pass.
        """
        now = datetime.now(timezone.utc)

        token_time = token.creation_time
        if token_time is None:
            token_time = token.recent_listing_time
        if token_time is None:
            # No age data — let it through, other filters will catch junk
            return (True, "")

        age_seconds = (now - token_time).total_seconds()
        if age_seconds > self.max_age_seconds:
            hours = age_seconds / 3600
            return (False, f"token age {hours:.1f}h > max — too old")
        return (True, "")

    def _check_mcap(self, token: ScannerToken) -> Tuple[bool, str]:
        if token.market_cap is None:
            return (False, "no market cap data")
        if token.market_cap < self.min_mcap:
            return (False, f"mcap ${token.market_cap:,.0f} < min")
        if token.market_cap > self.max_mcap:
            return (False, f"mcap ${token.market_cap:,.0f} > max")
        return (True, "")

    def _check_volume_5m(self, token: ScannerToken) -> Tuple[bool, str]:
        """Reject tokens with dead short-window volume."""
        vol = token.volume_5m_usd
        if vol is None:
            return (False, "no 5m volume data")
        if vol < self.min_volume_5m_usd:
            return (False, f"5m volume ${vol:,.0f} < min")
        return (True, "")

    def _check_liquidity(self, token: ScannerToken) -> Tuple[bool, str]:
        if token.liquidity is None or token.market_cap is None:
            return (False, "no liquidity data")
        if token.market_cap <= 0:
            return (False, "zero market cap")
        ratio = token.liquidity / token.market_cap
        token.liquidity_ratio = ratio
        if ratio < self.min_liquidity_ratio:
            return (False, f"liquidity ratio {ratio:.2%} < min")
        return (True, "")

    def _check_fees(self, token: ScannerToken) -> Tuple[bool, str]:
        """
        Fee generation check. Proxy: volume_24h × DEX fee rate.
        Tokens failing this are PERMANENTLY blacklisted — indicates rug/dead token.
        """
        if token.volume_24h_usd is None or token.market_cap is None:
            return (False, "no volume data for fee check")
        if self.sol_price_usd <= 0:
            return (False, "no SOL price available")

        min_fees_sol = get_min_fees_sol(token.market_cap)
        min_fees_usd = min_fees_sol * self.sol_price_usd
        estimated_fees_usd = token.volume_24h_usd * DEX_FEE_PROXY_PCT

        token.estimated_fees_sol = estimated_fees_usd / self.sol_price_usd if self.sol_price_usd > 0 else 0
        token.fee_check_passed = estimated_fees_usd >= min_fees_usd

        if not token.fee_check_passed:
            return (False,
                    f"fee generation {token.estimated_fees_sol:.2f} SOL < "
                    f"tier minimum for mcap ${token.market_cap:,.0f}")
        return (True, "")


# ═══════════════════════════════════════════════════════════════
# NARROW SCAN FILTERS
# ═══════════════════════════════════════════════════════════════

# Market-cap-tiered holder concentration ceilings.
# List of [mcap_ceiling_usd, max_top10_pct] pairs, ascending by ceiling.
HOLDER_TIERS = param("scanner.narrow.holder_tiers")              # tuned privately
HOLDER_FALLBACK_MAX_PCT = param("scanner.narrow.holder_fallback_max_pct")


def get_max_holder_pct(market_cap: float) -> float:
    """Look up max allowed top10 holder % for a given market cap."""
    for mcap_ceiling, max_pct in HOLDER_TIERS:
        if market_cap <= mcap_ceiling:
            return max_pct
    return HOLDER_FALLBACK_MAX_PCT


class NarrowScanFilters:
    """
    Safety checks using the provider's token-security data (one paid call).

    For paper trading (PumpFun only):
      - Holder concentration: check_holder_concentration(token)
          → passes if top10_pct <= tier_ceiling(mcap)   [tiers: XXXX]
        Prefers top-10 holder share (LP included); falls back to the
        top-10 "user" share if the former is unavailable.
      - Mint/freeze authority: SKIPPED (PumpFun guarantees revoked)
      - Bundler / fresh-wallet analysis: deferred to a later build

    Near-fail tolerance: tokens within holder_tolerance_pct of the ceiling are
    demoted back to Wide instead of permanently blacklisted.

    Extra fields stored but not filtered on (for future scoring):
      - creator_pct, transfer_fee_enabled, pre_market_holder_count
    """

    def __init__(
        self,
        holder_tolerance_pct: float = param("scanner.narrow.holder_tolerance_pct"),
    ):
        self.chain = FilterChain("narrow_scan")
        self.holder_tolerance_pct = holder_tolerance_pct

        # PumpFun paper trading: only holder concentration check
        self.chain.add("holder_concentration", self._check_holders)

    def run(self, token: ScannerToken) -> FilterResult:
        return self.chain.run(token)

    def _get_effective_holder_pct(self, token: ScannerToken) -> Optional[float]:
        """Best available holder concentration metric (see class docstring)."""
        if token.top10_holder_pct is not None:
            return token.top10_holder_pct
        return token.top10_user_pct

    def _get_threshold(self, token: ScannerToken) -> float:
        """Get the max allowed holder % based on token's market cap."""
        if token.market_cap is not None and token.market_cap > 0:
            return get_max_holder_pct(token.market_cap)
        return HOLDER_FALLBACK_MAX_PCT

    def _check_holders(self, token: ScannerToken) -> Tuple[bool, str]:
        pct = self._get_effective_holder_pct(token)
        if pct is None:
            return (True, "")   # No data yet — pass provisionally

        threshold = self._get_threshold(token)
        mcap_str = f"${token.market_cap:,.0f}" if token.market_cap else "unknown"

        if pct > threshold:
            # Within tolerance band → near-fail (demote), else hard fail (blacklist)
            tolerance_limit = threshold + self.holder_tolerance_pct
            if pct <= tolerance_limit:
                return (False,
                        f"top10 holders {pct:.1%} above tier ceiling for mcap "
                        f"{mcap_str} (within tolerance — demote)")
            else:
                return (False,
                        f"top10 holders {pct:.1%} far above tier ceiling for mcap "
                        f"{mcap_str} (hard fail — blacklist)")
        return (True, "")

    def is_near_fail(self, token: ScannerToken) -> bool:
        """Check if the token's holder concentration is within the tolerance band."""
        pct = self._get_effective_holder_pct(token)
        if pct is None:
            return False
        threshold = self._get_threshold(token)
        tolerance_limit = threshold + self.holder_tolerance_pct
        return (pct > threshold
                and pct <= tolerance_limit)
