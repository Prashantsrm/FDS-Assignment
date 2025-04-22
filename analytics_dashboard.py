#!/usr/bin/env python3
"""
Social Media Analytics Dashboard

This script launches an interactive web dashboard for visualizing social media analytics data.
It uses Dash and Plotly to create various charts for user activities and trending content.

Run:
    python analytics_dashboard.py
"""

import os
import sys
import dash
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import dcc, html, Input, Output
from scipy import stats

# Paths to data files
DATA_DIR = 'output'
USER_ACTIVITY_PATH = os.path.join(DATA_DIR, 'user_activity.txt')
TRENDING_CONTENT_PATH = os.path.join(DATA_DIR, 'trending_content.txt')

# Utility functions
def parse_user_activity(file_path):
    """Parse user activity data file into a DataFrame."""
    records = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    user_id, activity_data = line.strip().split('\t')
                    activity_counts = dict(
                        (k, int(v)) for k, v in (item.split(':') for item in activity_data.split(','))
                    )
                    records.append({
                        'user_id': user_id,
                        'posts': activity_counts.get('posts', 0),
                        'likes': activity_counts.get('likes', 0),
                        'comments': activity_counts.get('comments', 0),
                        'shares': activity_counts.get('shares', 0),
                    })
                except Exception as e:
                    print(f"Skipping invalid line: {line.strip()} | Error: {e}", file=sys.stderr)
    except FileNotFoundError:
        print(f"Warning: {file_path} not found.", file=sys.stderr)
    
    df = pd.DataFrame(records)
    if not df.empty:
        df['total_activity'] = df[['posts', 'likes', 'comments', 'shares']].sum(axis=1)
    return df

def parse_trending_content(file_path):
    """Parse trending content data file into a DataFrame."""
    records = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    content_id, engagement = line.strip().split('\t')
                    records.append({'content_id': content_id, 'engagement': int(engagement)})
                except Exception as e:
                    print(f"Skipping invalid line: {line.strip()} | Error: {e}", file=sys.stderr)
    except FileNotFoundError:
        print(f"Warning: {file_path} not found.", file=sys.stderr)
    
    return pd.DataFrame(records)

# Load data
print("Loading datasets...")
user_activity_df = parse_user_activity(USER_ACTIVITY_PATH)
trending_content_df = parse_trending_content(TRENDING_CONTENT_PATH)

# Initialize Dash app
app = dash.Dash(__name__, title='Social Media Analytics Dashboard')

# Define simple styles
container_style = {'padding': '20px', 'fontFamily': 'Arial, sans-serif'}

# Layout
app.layout = html.Div([
    html.H1('Social Media Analytics Dashboard', style={'textAlign': 'center'}),
    
    html.Div([
        html.H4(f"User Activity Records: {len(user_activity_df)}"),
        html.H4(f"Trending Content Items: {len(trending_content_df)}")
    ], style={'textAlign': 'center', 'marginBottom': '20px'}),

    dcc.Tabs([
        dcc.Tab(label='User Activity Overview', children=[
            html.Div([
                html.H3('Overall Activity Distribution'),
                dcc.Graph(id='activity-distribution-pie'),

                html.H3('User Activity Statistics'),
                dcc.Graph(id='user-activity-bar'),

                html.H3('Top Users by Total Activity'),
                dcc.Slider(id='top-users-slider', min=5, max=50, step=5, value=20,
                           marks={i: str(i) for i in range(5, 55, 5)}),
                dcc.Graph(id='top-users-bar')
            ])
        ]),
        dcc.Tab(label='Trending Content', children=[
            html.Div([
                html.H3('Top Trending Content'),
                dcc.Slider(id='top-content-slider', min=5, max=50, step=5, value=20,
                           marks={i: str(i) for i in range(5, 55, 5)}),
                dcc.Graph(id='trending-content-bar')
            ])
        ])
    ])
], style=container_style)

# Callbacks

@app.callback(Output('activity-distribution-pie', 'figure'), Input('activity-distribution-pie', 'id'))
def update_pie_chart(_):
    if user_activity_df.empty:
        return go.Figure()
    totals = user_activity_df[['posts', 'likes', 'comments', 'shares']].sum()
    fig = px.pie(names=totals.index, values=totals.values, title='Overall Distribution of Activities')
    return fig

@app.callback(Output('user-activity-bar', 'figure'), Input('user-activity-bar', 'id'))
def update_activity_bar(_):
    if user_activity_df.empty:
        return go.Figure()
    stats = {
        'Average': user_activity_df[['posts', 'likes', 'comments', 'shares']].mean(),
        'Median': user_activity_df[['posts', 'likes', 'comments', 'shares']].median(),
        'Max': user_activity_df[['posts', 'likes', 'comments', 'shares']].max()
    }
    fig = go.Figure()
    for stat, values in stats.items():
        fig.add_trace(go.Bar(name=stat, x=values.index, y=values.values))
    fig.update_layout(barmode='group', title='Activity Type Statistics')
    return fig

@app.callback(Output('top-users-bar', 'figure'), Input('top-users-slider', 'value'))
def update_top_users(top_n):
    if user_activity_df.empty:
        return go.Figure()
    top_users = user_activity_df.nlargest(top_n, 'total_activity')
    fig = px.bar(top_users, x='user_id', y='total_activity', title=f'Top {top_n} Active Users')
    fig.update_layout(xaxis={'categoryorder':'total descending'})
    return fig

@app.callback(Output('trending-content-bar', 'figure'), Input('top-content-slider', 'value'))
def update_trending_content(top_n):
    if trending_content_df.empty:
        return go.Figure()
    top_content = trending_content_df.nlargest(top_n, 'engagement')
    fig = px.bar(top_content, x='content_id', y='engagement', title=f'Top {top_n} Trending Content')
    fig.update_layout(xaxis={'categoryorder':'total descending'})
    return fig

# Run the server
if __name__ == "__main__":
    print("Dashboard running at http://127.0.0.1:8050/")
    app.run(debug=True)
