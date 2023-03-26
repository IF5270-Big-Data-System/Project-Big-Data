import time
from kafka import KafkaProducer
import yfinance as yf
from datetime import date
import json


current_date = date.today()
print(current_date)
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         x.encode('utf-8'))

energy_company_tickers = "XOM CVX BP BKR COP" # ExxonMobil (XOM), Chevron (CVX), BP plc (BP), Baker Hughes (BKR), and Conoco Phillips (COP)

topic_name = "energy_stocks"

while True:
	data = yf.download(tickers=energy_company_tickers, start=current_date, interval='2m') #use this one in case for real implementation

	data = data.reset_index(drop=False)
	if len(data) != 0:
		data['Datetime'] = data['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
		my_dict = data.to_dict()
		
		msg = json.dumps(my_dict)
		print(msg)
		producer.send(topic_name, key=b'Energy Stock Update', value=msg)
		producer.flush()
	else:
		msg = 'stock market is not open'
		producer.send(topic_name, key=b'Energy Stock Update', value=msg)

	print(f"Producing to {topic_name}")
	time.sleep(120)


