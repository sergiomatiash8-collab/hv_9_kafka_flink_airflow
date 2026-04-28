import sys
import os
from datetime import datetime

# Налаштовуємо шлях до кореня проєкту
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if project_root not in sys.path:
    sys.path.append(project_root)

def test_domain_integrity():
    try:
        print(f"[*] Перевірка імпортів із кореня: {project_root}")
        
        # Імпортуємо твої класи
        from src.domain.entities.tweet import Tweet
        from src.domain.value_objects.author_id import AuthorId
        from src.domain.value_objects.company import Company
        from src.domain.value_objects.priority import Priority

        print("[+] Усі модулі імпортовані успішно.")

        # Створюємо тестовий AuthorId
        test_author = AuthorId("115854") # ID для Apple з твого mapping
        
        # Створюємо об'єкт Tweet
        # Використовуємо Company.APPLE та Priority.HIGH (якщо в Priority інша назва — підправимо)
        tweet = Tweet(
            author_id=test_author,
            created_at=datetime.now(),
            text="Тестове повідомлення для Clean Architecture",
            company=Company.APPLE,
            priority=Priority.HIGH if hasattr(Priority, 'HIGH') else list(Priority)[0]
        )

        print(f"[+] УСПІХ: Об'єкт Tweet створено!")
        print(f"    - Автор: {tweet.author_id}")
        print(f"    - Компанія: {tweet.company.value}")
        print(f"    - Довжина тексту: {tweet.text_length}")
        
        return True

    except Exception as e:
        print(f"[-] ПОМИЛКА: {e}")
        # Виводимо тип помилки для легшої діагностики
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_domain_integrity()
    if not success:
        sys.exit(1)