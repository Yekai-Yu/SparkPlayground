from kafka import KafkaProducer
from json import dumps, loads
import csv
import time

producer = KafkaProducer(bootstrap_servers='b-1.mp2.zetw14.c11.kafka.us-east-1.amazonaws.com:9092,b-2.mp2.zetw14.c11.kafka.us-east-1.amazonaws.com:9092', value_serializer=lambda K:dumps(K).encode('utf-8'))


schema_key = ["DayOfWeek", "FlightDate", "UniqueCarrier","FlightNum","Origin","Dest","CRSDepTime","DepTime","DepDelay","CRSArrTime","ArrTime","ArrDelay"]


def read():
    with open('/home/ec2-user/final_v2.csv', 'r') as file:
        reader = csv.reader(file, delimiter = '\n')
        num = 0
        for messages in reader:
            msg = messages[0].split(",")
            kmsg = {}
            for k,v in enumerate(schema_key):
                kmsg[v] = msg[k]
            #print("Sending msg: \n" + str(kmsg))
            producer.send('AWSKafkaTutorialTopic', kmsg)
            # producer.flush()

            num+=1
            if (num % 10000 == 0):
                print(num)
         #   time.sleep(2)
            #print("One batch")
               

read()
