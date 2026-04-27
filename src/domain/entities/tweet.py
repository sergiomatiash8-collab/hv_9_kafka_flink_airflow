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
    Всі зміни стану створюють новий екземпляр (Immutability).
    """
    author_id: AuthorId
    created_at: datetime
    text: str
    company: Optional[Company] = None
    priority: Optional[Priority] = None

    def __post_init__(self):
        """Валідація бізнес-правил."""
        if not self.text or not self.text.strip():
            raise ValueError("Текст твіта не може бути порожнім")
        
        if len(self.text) > 280:
            raise ValueError("Текст твіта перевищує ліміт 280 символів")

    @property
    def text_length(self) -> int:
        return len(self.text)

    def enrich(self, company: Company, priority: Priority) -> 'Tweet':
        """
        Метод збагачення даних. 
        Використовуємо replace для збереження незмінності об'єкта.
        """
        return replace(self, company=company, priority=priority)