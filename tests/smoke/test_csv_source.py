import sys
import os
import csv

# Налаштовуємо шлях до кореня проєкту
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if project_root not in sys.path:
    sys.path.append(project_root)

def test_csv_availability():
    try:
        from src.infrastructure.config.settings import settings
        csv_path = os.path.join(project_root, settings.csv_file_path)
        
        print(f"[*] Перевірка доступу до файлу: {csv_path}")
        
        if not os.path.exists(csv_path):
            print(f"[-] ПОМИЛКА: Файл не знайдено за шляхом {csv_path}")
            return False
            
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            first_row = next(reader, None)
            if first_row:
                print(f"[+] Файл доступний. Перші ключі: {list(first_row.keys())}")
            else:
                print("[!] ПОПЕРЕДЖЕННЯ: Файл порожній.")
                
        print("[+] УСПІХ: Джерело даних готове!")
        return True
    except Exception as e:
        print(f"[-] ПОМИЛКА ЧИТАННЯ CSV: {e}")
        return False

if __name__ == "__main__":
    if test_csv_availability():
        sys.exit(0)
    else:
        sys.exit(1)