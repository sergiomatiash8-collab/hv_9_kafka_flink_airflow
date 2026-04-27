from abc import ABC, abstractmethod
from typing import Generic, TypeVar, List
from ..entities.tweet import Tweet

T = TypeVar('T')

class MessageProducer(ABC, Generic[T]):
    """
    Абстрактний інтерфейс (Port) для відправки повідомлень.
    
    Це "роз'єм", у який ми пізніше вставимо Kafka, RabbitMQ 
    або навіть простий логгер для тестів.
    """

    @abstractmethod
    def send(self, message: T) -> None:
        """Відправити одне повідомлення."""
        pass

    @abstractmethod
    def send_batch(self, messages: List[T]) -> None:
        """Відправити список повідомлень (bulk/batch)."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Закрити з'єднання та очистити ресурси."""
        pass

class TweetProducer(MessageProducer[Tweet]):
    """Спеціалізований інтерфейс для роботи саме з сутністю Tweet."""
    pass