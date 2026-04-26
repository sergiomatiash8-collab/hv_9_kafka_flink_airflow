import streamlit as st
from confluent_kafka import Consumer
import json
import time

st.set_page_config(page_title="Twitter Stream", layout="wide")
st.title("📊 Real-time Tweets Monitoring")

@st.cache_resource
def get_consumer():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'st-group-final-v3',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    c = Consumer(conf)
    c.subscribe(['enriched_tweets'])
    return c

consumer = get_consumer()

if 'data' not in st.session_state:
    st.session_state.data = []

msg = consumer.poll(0.1)
if msg is not None and not msg.error():
    try:
        val = json.loads(msg.value().decode('utf-8'))
        st.session_state.data.insert(0, val)
    except:
        pass

if st.session_state.data:
    st.table(st.session_state.data[:15])
else:
    st.write("Чекаю на дані з Kafka...")

time.sleep(1)
st.rerun()