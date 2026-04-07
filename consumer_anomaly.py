from kafka import KafkaConsumer
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na anomalie prędkości...")

user_timestamps = {}

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    now = datetime.fromisoformat(tx['timestamp'])

    if user_id not in user_timestamps:
        user_timestamps[user_id] = []
    user_timestamps[user_id].append(now)

    nowa_lista = []
    
    for t in user_timestamps[user_id]:
        if now - t < timedelta(seconds=60):
            nowa_lista.append(t)
    user_timestamps[user_id] = nowa_lista

    if len(user_timestamps[user_id]) > 3:
        print(f"ALERT: {user_id} | {len(user_timestamps[user_id])} transakcji w 60s")
