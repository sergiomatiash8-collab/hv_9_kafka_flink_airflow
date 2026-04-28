import os
import sys
import glob
import kagglehub
import pandas as pd

# Додаємо корінь проекту в sys.path, щоб бачити src
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.infrastructure.config.settings import settings

def run_download():
    try:
        print("🚀 Початок завантаження з Kaggle...")
        
        # Завантаження (використовуємо твою логіку)
        tmp_path = kagglehub.dataset_download("thoughtvector/customer-support-on-twitter")
        
        csv_files = glob.glob(os.path.join(tmp_path, "**", "*.csv"), recursive=True)
        if not csv_files:
            print("❌ Помилка: CSV файл у завантаженому архіві не знайдено!")
            return

        file_path = csv_files[0]
        print(f"📂 Файл знайдено у тимчасовій папці: {file_path}")

        # Читаємо тільки 1000 рядків для початку
        df = pd.read_csv(file_path, nrows=1000)
        
        # Фільтруємо потрібні колонки
        df_filtered = df[['author_id', 'created_at', 'text']]

        # Використовуємо шлях із наших налаштувань!
        # settings.csv_file_path у нас 'data/raw_tweets.csv'
        output_path = os.path.join(project_root, settings.csv_file_path)
        
        # Створюємо папку data, якщо її ще немає
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Зберігаємо
        df_filtered.to_csv(output_path, index=False)
        
        print(f"✅ УСПІХ! Дані збережено у: {output_path}")
        print(df_filtered.head(3))

    except Exception as e:
        print(f"[-] Сталася помилка: {e}")

if __name__ == "__main__":
    run_download()