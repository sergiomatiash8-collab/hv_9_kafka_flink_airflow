from abc import ABC, abstractmethod

class ITweetRepository(ABC):
    @abstractmethod
    def save(self, tweet):
        pass
