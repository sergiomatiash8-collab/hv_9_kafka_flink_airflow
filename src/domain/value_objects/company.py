from enum import Enum
from typing import Dict

class Company(Enum):
    """
    Company Value Object.
    Централізоване визначення компаній-об'єктів моніторингу.
    """
    AMAZON = "Amazon"
    APPLE = "Apple"
    UBER = "Uber"
    MICROSOFT = "Microsoft"
    OTHER = "Other"

    @classmethod
    def from_author_id(cls, author_id: str) -> 'Company':
        """
        Domain Logic: Відображення автора на компанію.
        Це ядро бізнес-правил, яке тепер легко тестувати юніт-тестами.
        """
        mapping: Dict[str, 'Company'] = {
            "115852": cls.AMAZON,
            "115854": cls.APPLE,
            "17919972": cls.UBER,
            "115873": cls.MICROSOFT,
        }
        return mapping.get(author_id, cls.OTHER)