from enum import Enum

class Priority(Enum):
    """
    Priority Value Object.
    Defines urgency level of tweet processing.
    """
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    NORMAL = "NORMAL"

    @classmethod
    def from_text(cls, text: str) -> 'Priority':
        """
        Domain logic: keyword-based text analysis.
        Allows automatic prioritization of data stream.
        """
        text_lower = text.lower()

        # Keywords for critical issues
        high_keywords = {'broken', 'bad', 'error', 'issue', 'bug', 'crash', 'terrible'}
        if any(keyword in text_lower for keyword in high_keywords):
            return cls.HIGH

        # Keywords for support-related messages
        medium_keywords = {'help', 'please', 'question', 'support', 'issue'}
        if any(keyword in text_lower for keyword in medium_keywords):
            return cls.MEDIUM

        return cls.NORMAL