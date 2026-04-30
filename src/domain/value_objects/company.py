from enum import Enum
from typing import Dict

class Company(Enum):
    """
    Company Value Object.
    Central definition of monitored companies.
    """
    AMAZON = "Amazon"
    APPLE = "Apple"
    UBER = "Uber"
    MICROSOFT = "Microsoft"
    OTHER = "Other"

    @classmethod
    def from_author_id(cls, author_id: str) -> 'Company':
        """
        Domain logic: mapping author to company.
        Core business rule that is easily unit-testable.
        """
        mapping: Dict[str, 'Company'] = {
            "115852": cls.AMAZON,
            "115854": cls.APPLE,
            "17919972": cls.UBER,
            "115873": cls.MICROSOFT,
        }
        return mapping.get(author_id, cls.OTHER)