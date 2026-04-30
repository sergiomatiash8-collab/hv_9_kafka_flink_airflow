import os
import sys
import glob
import kagglehub
import pandas as pd

# Add project root to sys.path to access src modules
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if project_root not in sys.path:
    sys.path.append(project_root)

from src.infrastructure.config.settings import settings

def run_download():
    try:
        print("Starting download from Kaggle...")

        # Download dataset
        tmp_path = kagglehub.dataset_download("thoughtvector/customer-support-on-twitter")

        csv_files = glob.glob(os.path.join(tmp_path, "**", "*.csv"), recursive=True)
        if not csv_files:
            print("Error: CSV file not found in downloaded archive!")
            return

        file_path = csv_files[0]
        print(f"File found in temp folder: {file_path}")

        # Read only first 1000 rows for initial load
        df = pd.read_csv(file_path, nrows=1000)

        # Filter required columns
        df_filtered = df[['author_id', 'created_at', 'text']]

        # Use configured path from settings
        output_path = os.path.join(project_root, settings.csv_file_path)

        # Create directory if it does not exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Save file
        df_filtered.to_csv(output_path, index=False)

        print(f"SUCCESS: Data saved to: {output_path}")
        print(df_filtered.head(3))

    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    run_download()