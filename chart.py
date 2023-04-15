import plotly.express as px
import pandas as pd
import streamlit as st

# Define function to generate line chart of average sentiment by day and party
def generate_line_chart(df):
    # Group by date and party, and calculate the average sentiment
    df_grouped = df.groupby([df['datetime'].dt.date, 'party']).mean().reset_index()

    # Define custom colors for parties
    party_colors = {'thaisangthai': 'navy', 'moveforward': 'orange', 'pheuthai': 'red', 'palangpracharath': 'blue'}

    # Create line chart
    fig = px.line(df_grouped, x='datetime', y='sentiment', color='party', color_discrete_map=party_colors,
                  title='Average Sentiment by Day and Party')
    return fig

# Define function to generate box plot of positivity rate by party
def generate_box_plot(df):
    # Calculate positivity rate by party
    df_positivity = df.groupby('party').apply(lambda x: (x['sentiment'] > 0).mean()).reset_index(name='positivity_rate')

   # Define custom colors for parties
    party_colors = {'thaisangthai': 'navy', 'moveforward': 'orange', 'pheuthai': 'red', 'palangpracharath': 'blue'}

    # Create box plot
    fig = px.box(df, x='party', y='sentiment', color='party', color_discrete_map=party_colors,
                 labels={'party': 'Party', 'sentiment': 'Sentiment'}, title='Positivity Rate by Party')
    fig.update_traces(quartilemethod='exclusive')
    fig.update_yaxes(range=[-1, 1])
    fig.add_scatter(x=df_positivity['party'], y=df_positivity['positivity_rate'], mode='markers',
                    name='Positivity Rate', marker=dict(color='black', size=10))

    return fig


# Define Streamlit app
def app():
    # Load data
    df = pd.read_csv('data.csv', date_parser='datetime')
    df = df.dropna(subset=['sentiment']).reset_index(drop=True)
    df['datetime'] = pd.to_datetime(df['datetime'])

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

if __name__ == '__main__':
    app()
