import sys
import os

from confluent_kafka import Producer
import time, requests


def get_from_api(api_url):
    response = requests.get(api_url)
    return str(response.json()).replace("None","'None'")

if __name__ == '__main__':
    topic = os.environ['CLOUDKARAFKA_TOPIC'].split(",")[0]
    api_url = os.environ['API_URL']

    conf = {
        'bootstrap.servers': os.environ['CLOUDKARAFKA_BROKERS'],
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
	'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': os.environ['CLOUDKARAFKA_USERNAME'],
        'sasl.password': os.environ['CLOUDKARAFKA_PASSWORD']
    }

    p = Producer(**conf)


    starttime = time.monotonic()
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d]\n' %
                             (msg.topic(), msg.partition()))
            
    while True:
        msg = get_from_api(api_url)
        print("sending data")
        print(msg)
        try:
            p.produce(topic, msg, callback=delivery_callback)
        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(p))
        p.poll(0)
        p.flush()
        time.sleep(60.0 - ((time.monotonic() - starttime) % 60.0))

