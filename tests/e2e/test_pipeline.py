import unittest
import psycopg2
import time

class TestFullPipelineE2E(unittest.TestCase):
    def setUp(self):
        """Підключення до бази через порт 5433."""
        # Використовуємо DSN рядок для стабільності на Windows
        self.dsn = "host=127.0.0.1 port=5433 dbname=tweets_db user=admin password=admin123"
        try:
            self.conn = psycopg2.connect(self.dsn)
            self.cur = self.conn.cursor()
        except Exception as e:
            # Виводимо помилку так, щоб вона не викликала UnicodeDecodeError
            error_msg = str(e).encode('ascii', 'replace').decode('ascii')
            print(f"\n❌ Помилка підключення: {error_msg}")
            self.fail(f"Не вдалося підключитися до Postgres на порту 5433: {error_msg}")

    def tearDown(self):
        if hasattr(self, 'conn'):
            self.cur.close()
            self.conn.close()

    def test_pipeline_execution(self):
        """Перевірка наявності даних у таблиці tweets_enriched."""
        print("\n[E2E] Перевіряю базу даних...")
        
        # Чекаємо трохи, щоб дані встигли пройти через Kafka та Worker
        print("[E2E] Очікування обробки даних (10 сек)...")
        time.sleep(10)

        try:
            self.cur.execute("SELECT COUNT(*) FROM tweets_enriched;")
            count = self.cur.fetchone()[0]
            print(f"[E2E] Знайдено записів: {count}")
            
            self.assertGreater(count, 0, "Пайплайн працює, але таблиця tweets_enriched порожня!")
        except psycopg2.Error as e:
            self.fail(f"Помилка при виконанні запиту до БД: {e}")

if __name__ == "__main__":
    unittest.main()