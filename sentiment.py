import pandas as pd
import re
import json
from pydruid.db import connect
from google.cloud import language_v1

from pytz import timezone
from datetime import datetime, timedelta, date

# create a connection to Druid broker node
conn = connect(host='localhost', port=8888, path='/druid/v2/sql/', scheme='http')

cursor = conn.cursor()
cursor.execute('SELECT * FROM pheuthai_demo2')
df_pheuthai = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
df_pheuthai = df_pheuthai.loc[:,['id','name','datetime','tweet','hashtags']]
df_pheuthai['party'] = 'pheuthai'

cursor = conn.cursor()
cursor.execute('SELECT * FROM palangpracharath_demo11')
df_palangpracharath = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
df_palangpracharath = df_palangpracharath.loc[:,['id','name','datetime','tweet','hashtags']]
df_palangpracharath['party'] = 'palangpracharath'

cursor = conn.cursor()
cursor.execute('SELECT * FROM thaisangthai_demo3')
df_thaisangthai = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
df_thaisangthai = df_thaisangthai.loc[:,['id','name','datetime','tweet','hashtags']]
df_thaisangthai['party'] = 'thaisangthai'

cursor = conn.cursor()
cursor.execute('SELECT * FROM moveforward_demo')
df_moveforward = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
df_moveforward = df_moveforward.loc[:,['id','name','datetime','tweet','hashtags']]
df_moveforward['party'] = 'moveforward'

df = pd.concat([df_thaisangthai, df_palangpracharath, df_pheuthai, df_moveforward]).reset_index(drop=True)
thai_tz = timezone('Asia/Bangkok')
df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
df['datetime'] = pd.to_datetime(df['datetime'].dt.tz_convert(thai_tz).dt.strftime('%Y-%m-%d %H:%M:%S'))
df = df.drop_duplicates(['id'], keep='first')

# Load configuration from file
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# Create a client to connect to the Google Cloud Natural Language API
client = language_v1.LanguageServiceClient.from_service_account_json(config['google_cloud_credentials_path'])

def analyze_sentiment(tweet_text):
    # Remove URLs, mentions, and hashtags
    tweet_text = re.sub(r'http\S+', '', tweet_text)
    tweet_text = re.sub(r'@[\u0E00-\u0E7F\w]+', '', tweet_text)
    tweet_text = re.sub(r'#[\u0E00-\u0E7F\w]+', '', tweet_text)

    # Call the Google Cloud Natural Language API to perform sentiment analysis
    document = language_v1.Document(content=tweet_text, type_=language_v1.Document.Type.PLAIN_TEXT)
    response = client.analyze_sentiment(document=document)
    sentiment = response.document_sentiment.score
    return sentiment

df['sentiment'] = df['tweet'].apply(analyze_sentiment)
df.to_csv('data.csv', index=False)