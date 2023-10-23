
import pandas as pd
import re
import sys
import json
from pytz import timezone
from datetime import datetime, timedelta, date
from google.cloud import language_v1
from pydruid.db import connect

import pandas as pd
import re
from google.cloud import language_v1

# Create Druid connection
conn = connect(host='localhost', port=8888, path='/druid/v2/sql/', scheme='http')

class SentimentAnalyzer:
    def __init__(self, config):
        self.config = config
        
        # Create a client to connect to the Google Cloud Natural Language API
        self.client = language_v1.LanguageServiceClient.from_service_account_json(config['google_cloud_credentials_path'])

    def analyze(self, text):
        document = language_v1.Document(
            content=self.clean_tweet(text), 
            type_=language_v1.Document.Type.PLAIN_TEXT
        )
        
        try:
            response = self.client.analyze_sentiment(document=document)
            return response.document_sentiment.score
        
        except Exception as e:
            print(f"Error in sentiment analysis: {e}")
            return None

def load_data(sql_query, party_name, params=None):
    cursor = conn.cursor()

    try:
        cursor.execute(sql_query, params)

    except Exception as e:
        print(f"Error executing Druid query: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    df = df[['id','name','datetime','tweet','hashtags']]
    df['party'] = party_name

    return df

if __name__ == '__main__':

    try:
        with open('config.json') as f:
            config = json.load(f)
    
    except Exception as e:
        print(f"Error loading config file: {e}")
        sys.exit(1)

    try:
        # Create analyzer
        analyzer = SentimentAnalyzer(config)

        # Load data
        df_pheuthai = load_data('SELECT * FROM pheuthai_demo2', 'pheuthai')
        df_palangpracharath = load_data('SELECT * FROM palangpracharath_demo11', 'palangpracharath')
        df_thaisangthai = load_data('SELECT * FROM thaisangthai_demo3')
        df_moveforward = load_data('SELECT * FROM moveforward_demo')

        # Concatenate data
        df = pd.concat([df_thaisangthai, df_palangpracharath, df_pheuthai, df_moveforward]).reset_index(drop=True)
        df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
        df['datetime'] = pd.to_datetime(df['datetime'].dt.tz_convert(timezone('Asia/Bangkok')).dt.strftime('%Y-%m-%d %H:%M:%S'))
        df = df.drop_duplicates(['id'], keep='first')

        # Calculate sentiment 
        df['sentiment'] = df['tweet'].apply(analyzer.analyze)

        # Save results
        df.to_csv('results.csv', index=False)

    except Exception as e:
        print(f"Unexpected error: {e}")