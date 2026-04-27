from typing import List
from ..entities.tweet import Tweet
from ..value_objects.company import Company
from ..value_objects.priority import Priority

class EnrichmentService:
    """
    Domain Service для оркестрації логіки збагачення.
    
    Відповідає за координацію між різними Value Objects 
    та сутністю Tweet.
    """

    def enrich_tweet(self, tweet: Tweet) -> Tweet:
        """
        Виконує повний цикл бізнес-збагачення одного твіта.
        """
        # 1. Визначаємо компанію на основі ID автора
        company = Company.from_author_id(tweet.author_id.value)
        
        # 2. Визначаємо пріоритет на основі тексту
        priority = Priority.from_text(tweet.text)
        
        # 3. Повертаємо новий екземпляр збагаченого твіта
        return tweet.enrich(company, priority)

    def batch_enrich(self, tweets: List[Tweet]) -> List[Tweet]:
        """
        Пакетне збагачення (для обробки масивів даних).
        """
        return [self.enrich_tweet(t) for t in tweets]