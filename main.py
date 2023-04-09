import twint
import json
import sys
import re
import io
import types
import socket

import asyncio
import uvloop

import logging
import tracemalloc

import openai
from kafka import KafkaConsumer, KafkaProducer
from json import JSONEncoder

from google.cloud import language_v1

import nest_asyncio
nest_asyncio.apply()

# Load configuration from file
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# Create a client to connect to the Google Cloud Natural Language API
client = language_v1.LanguageServiceClient.from_service_account_json(config['google_cloud_credentials_path'])

# Define a function to create a Twint configuration object
def create_twint_config(search_word, topic_name):
    c = twint.Config()
    c.Search = search_word
    c.Custom["tweet"] = ["id", "name", "date", "time", "tweet", "hashtags"]
    c.Store_json = True
    c.Output = f"{config['output_path']}/{topic_name}.json"
    c.Since = config['since']
    c.Lang = config['language']
    c.Hide_output = True
    return c

# Define the async function to handle tweets
async def handle_tweet(tweet, topic_name, producer):
    try:
        producer.send(topic_name, value=tweet)
        producer.flush()
    except Exception as e:
        print(f"Error occurred while processing tweet: {e}")

# Define a function to perform sentiment analysis using Google Cloud Natural Language API
def analyze_sentiment(tweet_text):
    # Remove URLs, mentions, and hashtags
    tweet_text = re.sub(r'http\S+', '', tweet_text)
    tweet_text = re.sub(r'@\w+', '', tweet_text)
    tweet_text = re.sub(r'#\w+', '', tweet_text)
    # Remove non-alphanumeric characters and extra spaces
    tweet_text = re.sub(r'[^\w\s]','',tweet_text)
    tweet_text = re.sub(r'\s+',' ',tweet_text)

    # Call the Google Cloud Natural Language API to perform sentiment analysis
    document = language_v1.Document(content=tweet_text, type_=language_v1.Document.Type.PLAIN_TEXT)
    response = client.analyze_sentiment(document=document)
    sentiment = response.document_sentiment.score
    return sentiment

# Define a function to search tweets for a given search word and handle the results
async def search_tweets(search_word, topic_name, producer):
    try:
        # Run twint search as coroutine
        config = create_twint_config(search_word, topic_name)
        twint.run.Search(config)

    except Exception as e:
        print(f"Error occurred while searching tweets for {search_word}: {e}")

    finally:
        with open(config.Output, 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                tweet_text = data['tweet']
                sentiment = analyze_sentiment(tweet_text)
                result = {"name": data['name'],
                          "date": data['date'],
                          "time": data['time'],
                          'tweet': tweet_text,
                          'sentiment': sentiment,
                          "hashtags": data['hashtags']}
                asyncio.ensure_future(handle_tweet(result, topic_name, producer))

async def main():
    # Create Kafka producer pool
    producer_pool = {search_word: KafkaProducer(
        bootstrap_servers=config['kafka']['bootstrap_servers'],
        request_timeout_ms=config['kafka']['request_timeout_ms'],
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    ) for search_word in config['search_words']}

    # Create the async tasks to handle tweets for each search word
    tasks = [search_tweets(search_word, config['topic_names'][i], producer_pool[search_word])
             for i, search_word in enumerate(config['search_words'])]
    
    # Execute tasks concurrently
    await asyncio.gather(*tasks)

    # Close all Kafka producers
    for producer in producer_pool.values():
        producer.close()

    # Stop the event loop
    asyncio.get_event_loop().stop()

if __name__ == '__main__':
    async def async_main():
        await main()

    asyncio.run(async_main())