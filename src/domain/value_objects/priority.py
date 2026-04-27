from enum import Enum

class Priority(Enum):
    """
    Priority Value Object.
    Визначає рівень терміновості обробки твіта.
    """
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    NORMAL = "NORMAL"

    @classmethod
    def from_text(cls, text: str) -> 'Priority':
        """
        Domain Logic: Аналіз тексту на наявність ключових слів.
        Дозволяє системі автоматично пріоритезувати потік даних.
        """
        text_lower = text.lower()

        # Ключові слова для критичних проблем
        high_keywords = {'broken', 'bad', 'error', 'issue', 'bug', 'crash', 'жахливо'}
        if any(keyword in text_lower for keyword in high_keywords):
            return cls.HIGH

        # Ключові слова для підтримки
        medium_keywords = {'help', 'please', 'question', 'допомога', 'питання'}
        if any(keyword in text_lower for keyword in medium_keywords):
            return cls.MEDIUM

        return cls.NORMAL