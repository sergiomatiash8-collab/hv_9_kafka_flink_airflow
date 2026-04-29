import unittest
import subprocess
import time

class TestFullPipelineE2E(unittest.TestCase):
    def test_pipeline_execution(self):
        """Перевірка даних через docker exec."""
        print("\n[E2E] Очікування обробки даних (10 сек)...")
        time.sleep(10)

        # Команда, яку ми раніше вводили вручну
        command = [
            "docker", "exec", "postgres", 
            "psql", "-U", "admin", "-d", "tweets_db", 
            "-t", "-c", "SELECT COUNT(*) FROM tweets_enriched;"
        ]

        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            count = int(result.stdout.strip())
            
            print(f"[E2E] Знайдено записів у базі: {count}")
            self.assertGreater(count, 0, "Таблиця порожня!")
            print("✅ ТЕСТ ПРОЙДЕНО УСПІШНО!")
            
        except subprocess.CalledProcessError as e:
            self.fail(f"❌ Помилка виконання docker exec: {e.stderr}")
        except ValueError:
            self.fail(f"❌ Не вдалося отримати число з бази. Відповідь: {result.stdout}")

if __name__ == "__main__":
    unittest.main()