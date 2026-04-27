import kagglehub
import pandas as pd
import os
import glob

def download_and_save():
    print("🚀 Завантаження з Kaggle...")
    # Завантажуємо датасет
    tmp_path = kagglehub.dataset_download("thoughtvector/customer-support-on-twitter")
    
    # Шукаємо будь-який .csv файл у завантаженій папці та її підпапках
    csv_files = glob.glob(os.path.join(tmp_path, "**", "*.csv"), recursive=True)
    
    if not csv_files:
        print("❌ Помилка: CSV файл не знайдено!")
        return

    # Беремо перший знайдений файл (зазвичай це twcs.csv)
    file_path = csv_files[0]
    print(f"📂 Знайдено файл: {file_path}")
    
    # Читаємо оригінал (тільки 1000 рядків для тесту)
    df = pd.read_csv(file_path, nrows=1000)
    
    # Фільтруємо колонки
    df_filtered = df[['author_id', 'created_at', 'text']]
    
    # Зберігаємо локально в нашу папку проекту
    local_name = "raw_tweets.csv"
    df_filtered.to_csv(local_name, index=False)
    
    print(f"✅ Файл успішно збережено локально як: {local_name}")
    print(df_filtered.head())

if __name__ == "__main__":
    download_and_save()