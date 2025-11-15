from kafka import KafkaProducer
import json, time, random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

symbols = ['AAPL', 'GOOG', 'AMZN', 'MSFT']

while True:
    data = {
        'symbol': random.choice(symbols),
        'price': round(random.uniform(100, 500), 2),
        'volume': random.randint(1, 1000),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    producer.send('stock_prices', value=data)
    print(f"Sent: {data}")
    time.sleep(1)
    
    
   
