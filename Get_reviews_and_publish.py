

from time import sleep

import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def get_reviews():
    recipies = []
    links = []
    salad_url = 'https://www.epicurious.com/recipes/food/views/chicken-tikka-109308'
    url = 'https://www.epicurious.com/recipes/food/views/chicken-tikka-109308'
    #print('Accessing list')

    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            for strong_tag in soup.find_all('div', class_="review-text"):
                print(strong_tag.text)
                links.append(strong_tag.text)
    except Exception as ex:
        print('Exception in get_recipes')
        print(str(ex))
    finally:
        return links

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


if __name__ == '__main__':
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }
    all_reviews = get_reviews()
    print(len(all_reviews))
    if len(all_reviews) > 0:
        kafka_producer = connect_kafka_producer()
        for review in all_reviews:
            #print('**************************************************************************************')
            #print(review.rstrip('\n'))
            publish_message(kafka_producer, 'newreviewsss', 'raw', review.strip())
        if kafka_producer is not None:
            kafka_producer.close()

