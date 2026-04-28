import csv
import os
from datetime import datetime
from src.domain.entities.tweet import Tweet

class CSVTweetRepository:
    def __init__(self, base_path: str = "output"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)

    def save(self, tweet: Tweet) -> None:
        # Формуємо ім'я файлу за поточною хвилиною: tweets_2023_10_27_14_30.csv
        filename = f"tweets_{datetime.now().strftime('%Y_%m_%d_%H_%M')}.csv"
        filepath = os.path.join(self.base_path, filename)
        
        file_exists = os.path.isfile(filepath)
        
        with open(filepath, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['author_id', 'created_at', 'text', 'company', 'priority'])
            
            writer.writerow([
                tweet.author_id, 
                tweet.created_at, 
                tweet.text, 
                tweet.company.value if tweet.company else '', 
                tweet.priority.value if tweet.priority else ''
            ])