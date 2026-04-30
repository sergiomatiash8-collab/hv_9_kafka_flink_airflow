from dataclasses import dataclass, replace
from datetime import datetime
from typing import Optional

from ..value_objects.author_id import AuthorId
from ..value_objects.company import Company
from ..value_objects.priority import Priority

@dataclass(frozen=True)
class Tweet:
    """
    Core Domain Entity.
    All state changes create a new instance (immutability).
    """
    author_id: AuthorId
    created_at: datetime
    text: str
    company: Optional[Company] = None
    priority: Optional[Priority] = None

    def __post_init__(self):
        """Business rules validation."""
        if not self.text or not self.text.strip():
            raise ValueError("Tweet text cannot be empty")

        if len(self.text) > 280:
            raise ValueError("Tweet text exceeds 280 character limit")

    @property
    def text_length(self) -> int:
        return len(self.text)

    def enrich(self, company: Company, priority: Priority) -> 'Tweet':
        """
        Enrichment method.
        Uses replace to preserve immutability.
        """
        return replace(self, company=company, priority=priority)