from typing import List
from ..entities.tweet import Tweet
from ..value_objects.company import Company
from ..value_objects.priority import Priority

class EnrichmentService:
    """
    Domain service for orchestrating enrichment logic.

    Responsible for coordinating different Value Objects
    and the Tweet entity.
    """

    def enrich_tweet(self, tweet: Tweet) -> Tweet:
        """
        Executes full business enrichment cycle for a single tweet.
        """
        # 1. Determine company based on author ID
        company = Company.from_author_id(tweet.author_id.value)

        # 2. Determine priority based on text content
        priority = Priority.from_text(tweet.text)

        # 3. Return new enriched tweet instance
        return tweet.enrich(company, priority)

    def batch_enrich(self, tweets: List[Tweet]) -> List[Tweet]:
        """
        Batch enrichment (for processing multiple records).
        """
        return [self.enrich_tweet(t) for t in tweets]