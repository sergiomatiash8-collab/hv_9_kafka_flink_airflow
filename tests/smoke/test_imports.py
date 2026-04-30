import sys
import os
from datetime import datetime

# Set up the path to the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if project_root not in sys.path:
    sys.path.append(project_root)

def test_domain_integrity():
    try:
        print(f"[*] Verifying imports from root: {project_root}")
        
        # Import domain classes
        from src.domain.entities.tweet import Tweet
        from src.domain.value_objects.author_id import AuthorId
        from src.domain.value_objects.company import Company
        from src.domain.value_objects.priority import Priority

        print("[+] All modules imported successfully.")

        # Create a test AuthorId
        test_author = AuthorId("115854") # Example ID for Apple mapping
        
        # Create a Tweet object
        # Using Company.APPLE and Priority.HIGH (with a fallback if enum names differ)
        tweet = Tweet(
            author_id=test_author,
            created_at=datetime.now(),
            text="Test message for Clean Architecture",
            company=Company.APPLE,
            priority=Priority.HIGH if hasattr(Priority, 'HIGH') else list(Priority)[0]
        )

        print(f"[+] SUCCESS: Tweet object created!")
        print(f"    - Author: {tweet.author_id}")
        print(f"    - Company: {tweet.company.value}")
        print(f"    - Text Length: {tweet.text_length}")
        
        return True

    except Exception as e:
        print(f"[-] ERROR: {e}")
        # Print traceback for easier diagnostics
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_domain_integrity()
    if not success:
        sys.exit(1)