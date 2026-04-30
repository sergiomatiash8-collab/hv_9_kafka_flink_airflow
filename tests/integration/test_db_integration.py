import sys
import os
# Add project root to path so imports work correctly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.infrastructure.repositories.postgres_repository import PostgresTweetRepository
from src.domain.entities.tweet import Tweet
from src.domain.value_objects.author_id import AuthorId
from src.domain.value_objects.company import Company
from src.domain.value_objects.priority import Priority
from datetime import datetime

def test_database_connection_and_save():
    print("[*] Testing database connection...")
    repo = PostgresTweetRepository()
    
    # Create a test tweet object
    test_tweet = Tweet(
        author_id=AuthorId("test_user_123"),
        created_at=datetime.now(),
        text="This is a test tweet for integration verification",
        company=Company.GOOGLE,  # Use existing Value Objects
        priority=Priority.HIGH
    )
    
    try:
        repo.save(test_tweet)
        print("[+] Success: Data saved in PostgreSQL!")
    except Exception as e:
        print(f"[-] Error: Failed to save data. Details: {e}")

if __name__ == "__main__":
    test_database_connection_and_save()