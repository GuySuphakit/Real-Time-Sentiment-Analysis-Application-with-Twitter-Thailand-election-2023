
import json
import os
import re
import requests

import matplotlib.pyplot as plt
import plotly.express as px
import pandas as pd
import numpy as np

import streamlit as st
from wordcloud import WordCloud
from pythainlp.tokenize import word_tokenize

# Define function to generate line chart of average sentiment by day and party
def generate_line_chart(df):
    # Group by date and party, and calculate the average sentiment
    df_grouped = df.groupby([df['datetime'].dt.date, 'party']).mean().reset_index()

    # Define custom colors for parties
    party_colors = {'thaisangthai': 'navy', 'moveforward': '#ff7f0e', 'pheuthai': 'red', 'palangpracharath': 'blue'}

    # Create line chart
    fig = px.line(df_grouped, x='datetime', y='sentiment', color='party', color_discrete_map=party_colors,
                  title='Average Sentiment by Day and Party')
    return fig

# Define function to generate box plot of positivity rate by party
def generate_box_plot(df):
    # Calculate positivity rate by party
    df_positivity = df.groupby('party').apply(lambda x: (x['sentiment'] > 0).mean()).reset_index(name='positivity_rate')

   # Define custom colors for parties
    party_colors = {'thaisangthai': 'navy', 'moveforward': '#ff7f0e', 'pheuthai': 'red', 'palangpracharath': 'blue'}

    # Create box plot
    fig = px.box(df, x='party', y='sentiment', color='party', color_discrete_map=party_colors,
                 labels={'party': 'Party', 'sentiment': 'Sentiment'}, title='Positivity Rate by Party')
    fig.update_traces(quartilemethod='exclusive')
    fig.update_yaxes(range=[-1, 1])
    fig.add_scatter(x=df_positivity['party'], y=df_positivity['positivity_rate'], mode='markers',
                    name='Positivity Rate', marker=dict(color='black', size=10))

    return fig

def generate_bar_chart(df):
    df = df.assign(hashtags=df['hashtags'].str.split()).explode('hashtags')
    df = df.dropna(subset=['hashtags'])
    hashtag_counts = df['hashtags'].value_counts().reset_index()
    hashtag_counts.columns = ['hashtag', 'count']
    top_hashtags = hashtag_counts[:10]
    fig = px.bar(top_hashtags, x='hashtag', y='count', title='Top 10 Hashtags', 
                 color='count', color_continuous_scale=px.colors.sequential.Blues,
                 text='count')

    fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')

    return fig

def remove_non_thai(text):
    pattern = re.compile(r'[^\u0E00-\u0E7F\s]') # Match non-Thai characters and whitespace
    return re.sub(pattern, '', text)

def generate_word_cloud(df):
    text = ' '.join(df['tweet'].tolist())
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'@[\u0E00-\u0E7F\w]+', '', text)
    text = re.sub(r'#[\u0E00-\u0E7F\w]+', '', text)
    text = remove_non_thai(text)  # remove non-Thai characters
    tokens = word_tokenize(text, engine='newmm')
    text = ' '.join(tokens)
    font_path = '/Library/Fonts/Sarabun-Regular.ttf'
    wordcloud = WordCloud(font_path=font_path, width=800, height=400, background_color='white', colormap='viridis').generate(text)
    fig, ax = plt.subplots(figsize=(16, 8))
    ax.imshow(wordcloud, interpolation='bilinear')
    ax.set_axis_off()
    return fig


def try_json_loads(x):
    try:
        return json.loads(x)
    except (json.JSONDecodeError, TypeError):
        return x

def app():
    df = pd.read_csv('data.csv', date_parser='datetime')
    df = df.dropna(subset=['sentiment']).reset_index(drop=True)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['hashtags'] = df['hashtags'].apply(try_json_loads)

    # Show raw data
    st.subheader('Raw Data')
    st.write(df)

    # Show line chart
    st.subheader('Average Sentiment by Day and Party')
    line_chart = generate_line_chart(df)
    st.plotly_chart(line_chart, use_container_width=True)

    # Show box plot
    st.subheader('Positivity Rate by Party')
    box_plot = generate_box_plot(df)
    st.plotly_chart(box_plot, use_container_width=True)
    
    # Show bar chart
    st.subheader('Top 10 Hashtags')
    bar_chart = generate_bar_chart(df)
    st.plotly_chart(bar_chart, use_container_width=True)
    
    # Show word cloud
    st.subheader('Word Cloud')
    word_cloud = generate_word_cloud(df)
    st.pyplot(word_cloud)

if __name__ == '__main__':
    app()
