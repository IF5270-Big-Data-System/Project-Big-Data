rom kafka import KafkaConsumer
import json
import numpy as np
import pickle
import time
import requests

consumer = KafkaConsumer(
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    max_poll_records=10
)
topic_name = 'tech_stocks'

# load trained model from pickle file
with open('model_tech.pkl', 'rb') as f:
    clf = pickle.load(f)

send1 ='Jual Saham teknologi Anda'
send2 ='Beli Saham teknologi anda' 
consumer.subscribe(topics=[topic_name])
consumer.subscription()

for message in consumer:
    data = json.loads(message.value)
    ticker_data = data['Close']  # extract close prices from the message data

    if len(ticker_data) > 0:
        # extract relevant features from the close prices
        low_high_range = max(ticker_data) - min(ticker_data)
        quarter_end = False
        if ticker_data.index[-1].is_quarter_end:
            quarter_end = True
        open_close = ticker_data[-1] - ticker_data[0]
        
        # classify the stock based on the extracted features using the trained random forest classifier
        X_new = np.array([[low_high_range, quarter_end, open_close]])
        y_pred = clf.predict(X_new)
        if y_pred == 0:
            classification = 'naik'
            base_url = 'https://api.telegram.org/bot5826728619:AAGdPhIx0t54GK_e07ma4nfp1M4A8m-cA4g/sendMessage?chat_id=-915121480&text="{}"'.format(send1)
            requests.get(base_url)
        else:
            classification = 'turun'
            base_url = 'https://api.telegram.org/bot5826728619:AAGdPhIx0t54GK_e07ma4nfp1M4A8m-cA4g/sendMessage?chat_id=-915121480&text="{}"'.format(send2)
            requests.get(base_url)
        # print out the classification along with metadata for the message

        print(f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
        print(f"Classification: {classification}")