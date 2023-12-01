
import json
import re
import io
import asyncio
import nest_asyncio
from dataclasses import dataclass
import logging

from twint import Config, run
from kafka import KafkaProducer
from google.cloud import language_v1
from dataclasses import dataclass

class TweetProcessor:
    def __init__(self, config_path='config.json'):
        self.config = self.load_config(config_path)
        self.client = language_v1.LanguageServiceClient.from_service_account_json(
            self.config['google_cloud_credentials_path']
        )
        self.producer_pool = self.create_producer_pool()

    def load_config(self, config_path):
        with open(config_path, 'r') as config_file:
            return json.load(config_file)

    def create_producer_pool(self):
        return {
            search_word: KafkaProducer(
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                request_timeout_ms=self.config['kafka']['request_timeout_ms'],
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            ) for search_word in self.config['search_words']
        }

    def create_twint_config(self, search_word, topic_name):
        c = twint.Config()
        c.Search = search_word
        c.Custom["tweet"] = ["id", "name", "date", "time", "tweet", "hashtags"]
        c.Store_json = True
        c.Output = f"{self.config['output_path']}/{topic_name}.json"
        c.Since = self.config['since']
        c.Lang = self.config['language']
        c.Hide_output = True
        return c

    def analyze_sentiment(self, tweet_text):
        # Remove URLs, mentions, and hashtags
        tweet_text = re.sub(r'http\S+', '', tweet_text)
        tweet_text = re.sub(r'@[\u0E00-\u0E7F\w]+', '', tweet_text)
        tweet_text = re.sub(r'#[\u0E00-\u0E7F\w]+', '', tweet_text)

        # Call the Google Cloud Natural Language API to perform sentiment analysis
        document = language_v1.Document(content=tweet_text, type_=language_v1.Document.Type.PLAIN_TEXT)
        response = self.client.analyze_sentiment(document=document)
        sentiment = response.document_sentiment.score
        return sentiment

    async def handle_tweet(self, tweet, topic_name, producer):
        try:
            producer.send(topic_name, value=tweet)
            producer.flush()
        except Exception as e:
            logging.error(f"Error occurred while processing tweet: {e}")

    async def search_tweets(self, search_word, topic_name, producer):
        try:
            config = self.create_twint_config(search_word, topic_name)
            twint.run.Search(config)
        except Exception as e:
            logging.error(f"Error occurred while searching tweets for {search_word}: {e}")
        finally:
            with open(config.Output, 'r', encoding='utf-8') as f:
                for line in f:
                    data = json.loads(line)
                    tweet_text = data['tweet']
                    sentiment = self.analyze_sentiment(tweet_text)
                    result = {"name": data['name'],
                              "date": data['date'],
                              "time": data['time'],
                              'tweet': tweet_text,
                              'sentiment': sentiment,
                              "hashtags": data['hashtags']}
                    asyncio.ensure_future(self.handle_tweet(result, topic_name, producer))

    async def main(self):
        tasks = [
            self.search_tweets(search_word, self.config['topic_names'][i], self.producer_pool[search_word])
            for i, search_word in enumerate(self.config['search_words'])
        ]

        await asyncio.gather(*tasks)

        for producer in self.producer_pool.values():
            producer.close()

        asyncio.get_event_loop().stop()

if __name__ == '__main__':

    nest_asyncio.apply()

    logging.basicConfig(level=logging.INFO)
    
    asyncio.run(TweetProcessor().main())