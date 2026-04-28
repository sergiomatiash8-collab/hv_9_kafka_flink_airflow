import sys
import os

# Налаштовуємо шлях до кореня проєкту
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if project_root not in sys.path:
    sys.path.append(project_root)

def test_settings_load():
    try:
        print("[*] Перевірка завантаження AppSettings...")
        from src.infrastructure.config.settings import settings
        
        print("[+] Клас settings завантажено успішно.")
        
        # Перевіряємо Kafka (звертаємось через settings.kafka)
        print(f"[*] Kafka Servers: {settings.kafka.bootstrap_servers}")
        print(f"[*] Kafka Topic:   {settings.kafka.topic}")
        
        # Перевіряємо Database (звертаємось через settings.database)
        print(f"[*] DB Host:       {settings.database.host}")
        print(f"[*] DB Name:       {settings.database.database}")
        
        # Перевіряємо, чи працює property для підключення
        print(f"[*] Conn String:   {settings.database.connection_string.replace(settings.database.password, '********')}")
        
        print("[+] УСПІХ: Конфігурація валідна!")
        return True
    except Exception as e:
        print(f"[-] ПОМИЛКА КОНФІГУРАЦІЇ: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if test_settings_load():
        sys.exit(0)
    else:
        sys.exit(1)