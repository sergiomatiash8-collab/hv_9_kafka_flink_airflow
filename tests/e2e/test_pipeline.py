import unittest
import subprocess
import time

class TestFullPipelineE2E(unittest.TestCase):
    def test_pipeline_execution(self):
        """Verify data via docker exec."""
        print("\n[E2E] Waiting for data processing (10 sec)...")
        time.sleep(10)

        # Command previously entered manually
        command = [
            "docker", "exec", "postgres", 
            "psql", "-U", "admin", "-d", "tweets_db", 
            "-t", "-c", "SELECT COUNT(*) FROM tweets_enriched;"
        ]

        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            count = int(result.stdout.strip())
            
            print(f"[E2E] Records found in database: {count}")
            self.assertGreater(count, 0, "Table is empty!")
            print("TEST PASSED SUCCESSFULLY!")
            
        except subprocess.CalledProcessError as e:
            self.fail(f"Error executing docker exec: {e.stderr}")
        except ValueError:
            self.fail(f"Could not retrieve count from database. Response: {result.stdout}")

if __name__ == "__main__":
    unittest.main()