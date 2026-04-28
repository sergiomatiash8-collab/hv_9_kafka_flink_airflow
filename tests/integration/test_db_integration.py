import sys
import os
# Додаємо корінь проєкту в шлях, щоб імпорти працювали
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.infrastructure.repositories.postgres_repository import PostgresTweetRepository
from src.domain.entities.tweet import Tweet
from src.domain.value_objects.author_id import AuthorId
from src.domain.value_objects.company import Company
from src.domain.value_objects.priority import Priority
from datetime import datetime

def test_database_connection_and_save():
    print("[*] Тестування підключення до БД...")
    repo = PostgresTweetRepository()
    
    # Створюємо тестовий об'єкт твіта
    test_tweet = Tweet(
        author_id=AuthorId("test_user_123"),
        created_at=datetime.now(),
        text="Це тестовий твіт для перевірки інтеграції",
        company=Company.GOOGLE,  # Заміни на ті, що є у твоїх Value Objects
        priority=Priority.HIGH
    )
    
    try:
        repo.save(test_tweet)
        print("[+ ] Успіх: Дані збережено в PostgreSQL!")
    except Exception as e:
        print(f"[-] Помилка: Не вдалося зберегти дані. Деталі: {e}")

if __name__ == "__main__":
    test_database_connection_and_save()