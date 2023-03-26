from kafka import KafkaConsumer
import json
import numpy as np
import pickle
import time
import requests
from joblib import dump, load

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                                                      auto_offset_reset='latest',
                                                       max_poll_records = 10)
topic_name='tech_stocks'

# load trained model from pickle file
#with open('model_tech.pkl', 'rb') as f:
clf = load('stockTechPredictModelSVM.pkl')

send1 ='Jual Saham teknologi Anda'
send2 ='Beli Saham teknologi anda' 
consumer.subscribe(topics=[topic_name])
consumer.subscription()

for message in consumer:

    data = message.value.decode('utf-8')
    my_dict = json.loads(data)
    open =[]
    close=[]
    low=[]
    high=[]
    for k,v in my_dict.items():
        if "Close" in k:
            close.append(v["0"])
        
        if "Open" in k:
            open.append(v["0"])
        
        if "Low" in k:
            low.append(v["0"])
        
        if "High" in k:
            high.append(v["0"])
        
    low_high = np.array(low) - np.array(high)
    open_close = np.array(open) - np.array(close[:5])

    # perlu penyesuaian juga
    X = np.array([low_high, open_close]) # [[1,2,3,4,5], [1,2,3,4,5]]
    y_pred = clf.predict(X) # array of [0,1,0,1,0]

    count_zeros = sum([1 for element in y_pred if element == 0]) #count num of zeros
    count_ones = sum([1 for element in y_pred if element == 1]) # count num of ones

    if count_zeros > count_ones:
            classification = 'naik'
            base_url = 'https://api.telegram.org/bot5826728619:AAGdPhIx0t54GK_e07ma4nfp1M4A8m-cA4g/sendMessage?chat_id=-915121480&text="{}"'.format(send1)
            requests.get(base_url)
    else:
            classification = 'turun'
            base_url = 'https://api.telegram.org/bot5826728619:AAGdPhIx0t54GK_e07ma4nfp1M4A8m-cA4g/sendMessage?chat_id=-915121480&text="{}"'.format(send2)
            requests.get(base_url)

    print(f"Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")
    print(f"Classification: {classification}")

