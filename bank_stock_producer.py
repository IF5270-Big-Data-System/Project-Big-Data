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

bank_company_tickers = "JPM WFC BAC C GS" # JPMorgan Chase (JPM), Wells Fargo (WFC), Bank of America (BAC), Citigroup (C), and Goldman Sachs (GS).

topic_name = "bank_stocks"

while True:
	data = yf.download(tickers=bank_company_tickers, start=current_date, interval='2m') #use this one in case for real implementation

	data = data.reset_index(drop=False)
	if len(data) != 0:
		data['Datetime'] = data['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
		my_dict = data.to_dict()
		
		msg = json.dumps(my_dict)
		print(msg)
		producer.send(topic_name, key=b'Bank Stock Update', value=msg)
		producer.flush()
	else:
		msg = 'stock market is not open'
		producer.send(topic_name, key=b'Bank Stock Update', value=msg)

	print(f"Producing to {topic_name}")
	time.sleep(120)


