import sys
import os

# Автоматично знаходимо корінь проєкту (на два рівні вгору від цього файлу)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)

def test_domain_integrity():
    try:
        print(f"[*] Root: {project_root}")
        print("[*] Перевірка імпорту доменних об'єктів...")
        
        from src.domain.entities.tweet import Tweet
        from src.domain.value_objects.author_id import AuthorId
        from src.domain.value_objects.company import Company
        from src.domain.value_objects.priority import Priority
        from datetime import datetime

        # Пробуємо створити об'єкт
        tweet = Tweet(
            author_id=AuthorId("user123"),
            created_at=datetime.now(),
            text="Hello, Clean Architecture!",
            company=Company.GOOGLE,
            priority=Priority.HIGH
        )
        
        print(f"[+] УСПІХ: Tweet створено для {tweet.author_id}")
        return True
    except Exception as e:
        print(f"[-] ПОМИЛКА ТЕСТУ: {e}")
        return False

if __name__ == "__main__":
    if test_domain_integrity():
        sys.exit(0)
    else:
        sys.exit(1)