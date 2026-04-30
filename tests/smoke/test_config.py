import sys
import os

# Set up the path to the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if project_root not in sys.path:
    sys.path.append(project_root)

def test_settings_load():
    try:
        print("[*] Verifying AppSettings loading...")
        from src.infrastructure.config.settings import settings
        
        print("[+] Settings class loaded successfully.")
        
        # Verify Kafka settings
        print(f"[*] Kafka Servers: {settings.kafka.bootstrap_servers}")
        print(f"[*] Kafka Topic:   {settings.kafka.topic}")
        
        # Verify Database settings
        print(f"[*] DB Host:       {settings.database.host}")
        print(f"[*] DB Name:       {settings.database.database}")
        
        # Verify connection string property (masking password)
        masked_conn = settings.database.connection_string.replace(settings.database.password, '********')
        print(f"[*] Conn String:   {masked_conn}")
        
        print("[+] SUCCESS: Configuration is valid!")
        return True
    except Exception as e:
        print(f"[-] CONFIGURATION ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if test_settings_load():
        sys.exit(0)
    else:
        sys.exit(1)