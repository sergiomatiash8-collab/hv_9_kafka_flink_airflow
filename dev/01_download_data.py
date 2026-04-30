import kagglehub
import pandas as pd
import os
import glob

def download_and_save():
    print("Downloading from Kaggle...")

    # Download dataset
    tmp_path = kagglehub.dataset_download("thoughtvector/customer-support-on-twitter")

    # Search for any .csv file in the downloaded folder and subfolders
    csv_files = glob.glob(os.path.join(tmp_path, "**", "*.csv"), recursive=True)

    if not csv_files:
        print("Error: CSV file not found!")
        return

    # Take the first found file (usually twcs.csv)
    file_path = csv_files[0]
    print(f"File found: {file_path}")

    # Read original file (only first 1000 rows for testing)
    df = pd.read_csv(file_path, nrows=1000)

    # Filter required columns
    df_filtered = df[['author_id', 'created_at', 'text']]

    # Save locally to project folder
    local_name = "raw_tweets.csv"
    df_filtered.to_csv(local_name, index=False)

    print(f"File successfully saved locally as: {local_name}")
    print(df_filtered.head())

if __name__ == "__main__":
    download_and_save()