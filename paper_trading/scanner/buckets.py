"""
Bucket System
==============
Manages token capacity across funnel tiers.

Funnel tiers (capacities and TTLs from config → funnels.buckets, redacted):
  WIDE:   XXXX slots, XXXX TTL — borderline tokens awaiting re-evaluation
  NARROW: XXXX slots, XXXX TTL — passed Wide eval, awaiting safety checks
  FULL:   XXXX slots, XXXX TTL — active WebSocket monitoring

Each bucket enforces:
  - Maximum capacity
  - TTL (time-to-live) per token
  - Fill level tracking for adaptive polling decisions
  - Ordered token tracking (insertion order for FIFO when needed)
"""

import logging
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from paper_trading.config.params import param
from paper_trading.scanner.token_model import ScannerToken, Tier

logger = logging.getLogger(__name__)


class Bucket:
    def __init__(self, name: str, capacity: int, ttl_seconds: int = 0):
        self.name = name
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self._tokens: OrderedDict[str, ScannerToken] = OrderedDict()

    @property
    def count(self) -> int:
        return len(self._tokens)

    @property
    def available_slots(self) -> int:
        return max(0, self.capacity - self.count)

    @property
    def is_full(self) -> bool:
        return self.count >= self.capacity

    @property
    def fill_pct(self) -> float:
        return self.count / self.capacity if self.capacity > 0 else 1.0

    @property
    def is_depleted(self) -> bool:
        return self.fill_pct <= param("funnels.buckets.depleted_fill_pct")

    @property
    def is_critical(self) -> bool:
        if self.name == "full":
            return self.fill_pct <= param("funnels.buckets.critical_fill_pct_full")
        return self.fill_pct <= param("funnels.buckets.critical_fill_pct")

    def add(self, token: ScannerToken) -> bool:
        if token.token_ca in self._tokens:
            return False
        if self.is_full:
            return False
        self._tokens[token.token_ca] = token
        return True

    def remove(self, token_ca: str) -> Optional[ScannerToken]:
        return self._tokens.pop(token_ca, None)

    def get(self, token_ca: str) -> Optional[ScannerToken]:
        return self._tokens.get(token_ca)

    def contains(self, token_ca: str) -> bool:
        return token_ca in self._tokens

    def get_all(self) -> List[ScannerToken]:
        return list(self._tokens.values())

    def get_all_cas(self) -> List[str]:
        return list(self._tokens.keys())

    def enforce_ttl(self) -> List[ScannerToken]:
        if self.ttl_seconds <= 0:
            return []
        now = datetime.now(timezone.utc)
        expired = []
        for ca, token in list(self._tokens.items()):
            age_in_tier = (now - token.tier_entry_time).total_seconds()
            if age_in_tier > self.ttl_seconds:
                expired.append(self._tokens.pop(ca))
        if expired:
            logger.info(
                f"Bucket '{self.name}': {len(expired)} tokens expired "
                f"(TTL={self.ttl_seconds}s)"
            )
        return expired

    def evict_oldest(self) -> Optional[ScannerToken]:
        if not self._tokens:
            return None
        ca, token = next(iter(self._tokens.items()))
        return self._tokens.pop(ca)

    def status(self) -> dict:
        return {
            "name": self.name, "count": self.count,
            "capacity": self.capacity, "fill_pct": round(self.fill_pct, 2),
            "available": self.available_slots,
            "is_depleted": self.is_depleted, "is_critical": self.is_critical,
            "ttl_seconds": self.ttl_seconds,
        }

    def clear(self):
        self._tokens.clear()


class BucketSet:
    def __init__(
        self,
        wide_capacity: int = param("funnels.defaults.wide_capacity"),
        narrow_capacity: int = param("funnels.defaults.narrow_capacity"),
        full_capacity: int = param("funnels.defaults.full_capacity"),
        wide_ttl: int = param("funnels.buckets.wide_ttl_seconds"),
        narrow_ttl: int = param("funnels.buckets.narrow_ttl_seconds"),
        full_ttl: int = param("funnels.buckets.full_ttl_seconds"),
    ):
        self.wide = Bucket("wide", wide_capacity, wide_ttl)
        self.narrow = Bucket("narrow", narrow_capacity, narrow_ttl)
        self.full = Bucket("full", full_capacity, full_ttl)

    def find_token(self, token_ca: str) -> Optional[Tuple[Bucket, ScannerToken]]:
        for bucket in [self.full, self.narrow, self.wide]:
            token = bucket.get(token_ca)
            if token is not None:
                return (bucket, token)
        return None

    def contains(self, token_ca: str) -> bool:
        return (self.wide.contains(token_ca)
                or self.narrow.contains(token_ca)
                or self.full.contains(token_ca))

    def enforce_all_ttls(self) -> List[ScannerToken]:
        expired = []
        expired.extend(self.wide.enforce_ttl())
        expired.extend(self.narrow.enforce_ttl())
        expired.extend(self.full.enforce_ttl())
        return expired

    def total_tokens(self) -> int:
        return self.wide.count + self.narrow.count + self.full.count

    def status(self) -> dict:
        return {
            "wide": self.wide.status(), "narrow": self.narrow.status(),
            "full": self.full.status(), "total": self.total_tokens(),
        }
