from abc import ABC, abstractmethod
from typing import Generic, TypeVar, List
from ..entities.tweet import Tweet

T = TypeVar('T')

class MessageProducer(ABC, Generic[T]):
    """
    Abstract interface (Port) for sending messages.

    This is a "socket" that can later be implemented by Kafka, RabbitMQ,
    or even a simple logger for testing.
    """

    @abstractmethod
    def send(self, message: T) -> None:
        """Send a single message."""
        pass

    @abstractmethod
    def send_batch(self, messages: List[T]) -> None:
        """Send a list of messages (bulk/batch)."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close connection and release resources."""
        pass

class TweetProducer(MessageProducer[Tweet]):
    """Specialized interface for working specifically with Tweet entity."""
    pass