import streamlit as st
from confluent_kafka import Consumer
import json

# 1. Налаштування підключення до Kafka
conf = {
    'bootstrap.servers': 'localhost:9092', # Адреса брокера
    'group.id': 'streamlit-group',         # Унікальне ім'я нашого "слухача"
    'auto.offset.reset': 'earliest'        # Читати з самого початку, якщо ми вперше
}

consumer = Consumer(conf)
consumer.subscribe(['enriched_tweets']) # ПІДПИСКА НА ТОПІК

st.title("📊 Real-time Tweets Monitoring")
st.write("Чекаємо на дані зі збагаченого топіка Kafka...")

# Місце (контейнер) в інтерфейсі, яке ми будемо постійно оновлювати
table_placeholder = st.empty()
all_data = []

# 2. Нескінченний цикл прослуховування
try:
    while True:
        # Питаємо у Kafka: "Чи є щось нове?" (чекаємо максимум 1 секунду)
        msg = consumer.poll(1.0)

        if msg is None:
            continue # Якщо порожньо — йдемо на наступне коло
        
        # Декодуємо дані з байтів у JSON
        tweet_data = json.loads(msg.value().decode('utf-8'))
        
        # Додаємо в початок списку (щоб свіжі були зверху)
        all_data.insert(0, tweet_data)
        
        # 3. Оновлюємо таблицю в браузері
        with table_placeholder.container():
            st.table(all_data[:10]) # Показуємо тільки останні 10 штук
            
except KeyboardInterrupt:
    consumer.close()