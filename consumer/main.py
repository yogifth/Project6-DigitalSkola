from kafka import KafkaConsumer
import json
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://postgres:password@localhost:1234/postgres")

# Data Structure
'''
{
    'core': {
        'page_type': str,
        'page_url': str
    },
    'user': {
        'user_id': int,
        'session_id': str
    },
    'event_timestamp': int
}
'''

consumer = KafkaConsumer(
    'ecommerce.tracker',
    bootstrap_servers= ['kafka:9092'],
    value_deserializer= lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    json_data = message.value
    result = {
        'page_type': json_data['core']['page_type'],
        'page_url': json_data['core']['page_url'],
        'user_id': json_data['user']['user_id'],
        'session_id': json_data['user']['session_id'],
        'event_timestamp': json_data['event_timestamp']
    }

with engine.begin() as conn:
    conn.execute(
        text('INSERT INTO project_6_result VALUES (:page_type, :page_url, :user_id, :session_id, event_timestamp)'),
        [result]
    )