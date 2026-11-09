"""
Blacklist Manager
==================
Tracks tokens that should be excluded from the scanner funnel.
Two types: cooldown (duration from config) and permanent (rug/scam).
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Set

logger = logging.getLogger(__name__)

from paper_trading.config.params import param

DEFAULT_COOLDOWN_HOURS = param("scanner.blacklist_cooldown_hours")


@dataclass
class BlacklistEntry:
    token_ca: str
    reason: str
    added_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = None
    permanent: bool = False

    @property
    def is_expired(self) -> bool:
        if self.permanent:
            return False
        if self.expires_at is None:
            return False
        return datetime.now(timezone.utc) > self.expires_at


class BlacklistManager:
    def __init__(self, cooldown_hours: int = DEFAULT_COOLDOWN_HOURS):
        self.cooldown_hours = cooldown_hours
        self._entries: Dict[str, BlacklistEntry] = {}

    def add_cooldown(self, token_ca: str, reason: str = "traded"):
        expires = datetime.now(timezone.utc) + timedelta(hours=self.cooldown_hours)
        self._entries[token_ca] = BlacklistEntry(
            token_ca=token_ca, reason=reason,
            expires_at=expires, permanent=False,
        )

    def add_permanent(self, token_ca: str, reason: str = "rug"):
        self._entries[token_ca] = BlacklistEntry(
            token_ca=token_ca, reason=reason, permanent=True,
        )

    def is_blacklisted(self, token_ca: str) -> bool:
        entry = self._entries.get(token_ca)
        if entry is None:
            return False
        if entry.is_expired:
            del self._entries[token_ca]
            return False
        return True

    def remove(self, token_ca: str):
        self._entries.pop(token_ca, None)

    def cleanup_expired(self) -> int:
        expired = [ca for ca, e in self._entries.items() if e.is_expired]
        for ca in expired:
            del self._entries[ca]
        return len(expired)

    @property
    def count(self) -> int:
        return len(self._entries)

    @property
    def permanent_count(self) -> int:
        return sum(1 for e in self._entries.values() if e.permanent)

    @property
    def cooldown_count(self) -> int:
        return sum(1 for e in self._entries.values() if not e.permanent)

    def status(self) -> dict:
        return {"total": self.count, "permanent": self.permanent_count,
                "cooldown": self.cooldown_count}
