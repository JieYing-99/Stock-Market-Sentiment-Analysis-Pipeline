from dash import Dash, html, dcc, Input, Output, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objs as go

from druid_query import get_twitter_top_trending_stocks, get_market_news_content, get_stock_news_content, get_twitter_content, get_market_news_sentiment_count, get_stock_news_sentiment_count, get_twitter_sentiment_count, get_tweets_time_series

THEME = dbc.themes.DARKLY
TEMPLATE = 'plotly_dark'
REFRESH_INTERVAL = 60*1000 # in milliseconds
DEFAULT_TIME_PERIOD = 'last_15_mins'

app = Dash(__name__, external_stylesheets=[THEME])

app.layout = dbc.Container(
    [
        dcc.Interval(
            id='refresh_interval',
            interval=REFRESH_INTERVAL,
            n_intervals=0
        ),
        html.H3('US Stock Market Sentiment', style={'marginBottom': 25, 'marginTop': 50}),
        html.Hr(),
        dbc.Container(
            [
                html.H4('Overview'),
                dbc.InputGroup(
                    [
                        dbc.InputGroupText("Period"),
                        dbc.Select(
                            id="select_overview_interval",
                            options=[
                                {"label": "Last 15 minutes", "value": "last_15_mins"},
                                {"label": "Last 30 minutes", "value": "last_30_mins"},
                                {"label": "Last 1 hour", "value": "last_1_hr"},
                                {"label": "Last 24 hours", "value": "last_24_hrs"},
                                {"label": "Today", "value": "today"},
                                {"label": "Last 3 days", "value": "last_3_days"}
                            ],
                            value=DEFAULT_TIME_PERIOD
                        )
                    ], style={'marginBottom': 20, 'marginTop': 10}
                ),
                dbc.Row(
                    [
                        dbc.Col(id="get_twitter_top_trending_stocks"),
                        dbc.Col(id="get_market_news_sentiment_count", width=6)
                    ]
                ),
                html.Br(),
                dbc.Row(id="get_market_news_content"),
                html.Br(),
                html.Hr(),
                html.H4('Stocks'),
                dbc.InputGroup(
                    [
                        dbc.InputGroupText("Ticker Symbol"),
                        dbc.Input(id="input_stocks_ticker_symbol", placeholder="Enter ticker symbol", type="text")
                    ]
                ),
                dbc.InputGroup(
                    [
                        dbc.InputGroupText("Period"),
                        dbc.Select(
                            id="select_stocks_interval",
                            options=[
                                {"label": "Last 15 minutes", "value": "last_15_mins"},
                                {"label": "Last 30 minutes", "value": "last_30_mins"},
                                {"label": "Last 1 hour", "value": "last_1_hr"},
                                {"label": "Last 24 hours", "value": "last_24_hrs"},
                                {"label": "Today", "value": "today"},
                                {"label": "Last 3 days", "value": "last_3_days"}
                            ],
                            value=DEFAULT_TIME_PERIOD
                        )
                    ], style={'marginBottom': 20, 'marginTop': 10}
                ),
                dbc.Row(
                    [
                        dbc.Col(id="get_twitter_sentiment_count", width=6),
                        dbc.Col(id="get_stock_news_sentiment_count", width=6)
                    ]
                ),
                html.Br(),
                dbc.Row(dbc.Col(id="get_twitter_content")),
                html.Br(),
                dbc.Row(dbc.Col(id="get_stock_news_content")),
                html.Br(),
                dbc.InputGroup(
                    [
                        dbc.InputGroupText("Ticker Symbol"),
                        dbc.Input(id="input_stocks_time_series_ticker_symbol", placeholder="Enter ticker symbol", type="text")
                    ]
                ),
                dbc.InputGroup(
                    [
                        dbc.InputGroupText("Granularity"),
                        dbc.RadioItems(
                            options=[
                                {"label": "Hour", "value": "hour"},
                                {"label": "Day", "value": "day"},
                            ],
                            value="hour",
                            id="radioitems_time_series_granularity",
                            inline=True,
                            style={'marginBottom': 2, 'marginTop': 8, 'marginLeft': 15}
                        )
                    ], style={'marginBottom': 20, 'marginTop': 10}
                ),
                dbc.Row(id="get_tweets_time_series")
            ]
        )
    ]
)

def plot_bar_chart(data, label_col_name, value_col_name, title):
    labels = data[label_col_name]
    values = data[value_col_name]
    fig = go.Figure(data=go.Bar(x=values, y=labels, orientation='h'))
    fig.update_layout(title_text=title, template=TEMPLATE)
    return fig

def plot_donut_chart(data, label_col_name, value_col_name, title):
    labels = data[label_col_name]
    values = data[value_col_name]
    fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.4)])
    fig.update_layout(title_text=title, template=TEMPLATE)
    fig.update_layout(annotations=[{'text': f'Total: {sum(values):,}', 'x': 0.5, 'y': 0.5, 'font_size': 11, 'showarrow': False}])
    return fig

def plot_multi_line_chart(data, label_col_name, value_col_names, title):
    labels = data[label_col_name]
    values = data[value_col_names]
    fig = go.Figure()
    for col in values.columns:
        fig.add_trace(go.Scatter(x=labels, y=values[col], mode='lines+markers', name=col))
        fig.update_layout(title_text=title, template=TEMPLATE)
    return fig

def create_table(data, columns, title):
    return html.Div([
        html.H5(title),
        dash_table.DataTable(
            data=data.to_dict('records'),
            columns=columns,
            style_header=dict(backgroundColor='rgb(30, 30, 30)', color="white"),
            style_data=dict(backgroundColor='rgb(50, 50, 50)', color="white", whiteSpace='normal', height='auto'),
            style_cell=dict(textAlign='left'),
            style_as_list_view=True,
            page_size=10
        )
    ])

def create_alert(message):
    alert_message_color_mapping = {
        'Pending input for Ticker Symbol': 'primary',
        'Pending data': 'warning'
    }
    return dbc.Alert(message, color=alert_message_color_mapping[message], style={'text-align': 'center'})

@app.callback(Output('get_twitter_top_trending_stocks', 'children'), 
[Input('select_overview_interval', 'value'), Input('refresh_interval', 'n_intervals')])
def create_twitter_top_trending_stocks_bar_chart(interval, refresh_interval):
    data = get_twitter_top_trending_stocks(interval)
    if data is not None:
        fig = plot_bar_chart(data=data.sort_values(by=['count'], ascending=True), label_col_name='ticker_symbol', value_col_name='count', title='Top Trending Stocks on Twitter')
        return dcc.Graph(figure=fig)
    else:
        return create_alert('Pending data')

@app.callback(Output('get_market_news_sentiment_count', 'children'), 
[Input('select_overview_interval', 'value'), Input('refresh_interval', 'n_intervals')])
def create_market_news_sentiment_count_donut_chart(interval, refresh_interval):
    data = get_market_news_sentiment_count(interval)
    if data is not None:
        fig = plot_donut_chart(data=data, label_col_name='sentiment', value_col_name='count', title='Market News Count by Sentiment')
        return dcc.Graph(figure=fig)
    else:
        return create_alert('Pending data')

@app.callback(Output('get_market_news_content', 'children'), 
[Input('select_overview_interval', 'value'), Input('refresh_interval', 'n_intervals')])
def create_market_news_content_table(interval, refresh_interval):
    data = get_market_news_content(interval)
    if data is not None:
        data = data.sort_values(by=['date'], ascending=False)
        data['Headline'] = data.apply(lambda row: f"[{row['title']}]({row['link']})", axis=1)
        data.rename(columns={'date': 'Date', 'sentiment': 'Sentiment'}, inplace=True)
        data = data[['Date', 'Headline', 'Sentiment']]
        columns = [
            {'name': 'Date', 'id': 'Date'},
            {'name': 'Headline', 'id': 'Headline', 'presentation': 'markdown'},
            {'name': 'Sentiment', 'id': 'Sentiment'}
        ]
        return create_table(data=data, columns=columns, title='Market News')
    else:
        return create_alert('Pending data')

@app.callback(Output('get_twitter_sentiment_count', 'children'), 
[Input('input_stocks_ticker_symbol', 'value'), Input('select_stocks_interval', 'value'), Input('refresh_interval', 'n_intervals')])
def create_twitter_sentiment_count_donut_chart(ticker_symbol, interval, refresh_interval):
    if ticker_symbol is not None:
        data = get_twitter_sentiment_count(ticker_symbol, interval)
        if data is not None:
            fig = plot_donut_chart(data=data, label_col_name='sentiment', value_col_name='count', title=f'{ticker_symbol} Tweets Count by Sentiment')
            return dcc.Graph(figure=fig)
        else:
            return create_alert('Pending data')
    else:
        return create_alert('Pending input for Ticker Symbol')

@app.callback(Output('get_stock_news_sentiment_count', 'children'), 
[Input('input_stocks_ticker_symbol', 'value'), Input('select_stocks_interval', 'value'), Input('refresh_interval', 'n_intervals')])
def create_stock_news_sentiment_count_donut_chart(ticker_symbol, interval, refresh_interval):
    if ticker_symbol is not None:
        data = get_stock_news_sentiment_count(ticker_symbol, interval)
        if data is not None:
            fig = plot_donut_chart(data=data, label_col_name='sentiment', value_col_name='count', title=f'{ticker_symbol} News Count by Sentiment')
            return dcc.Graph(figure=fig)
        else:
            return create_alert('Pending data')
    else:
        return create_alert('Pending input for Ticker Symbol')

@app.callback(Output('get_twitter_content', 'children'), 
[Input('input_stocks_ticker_symbol', 'value'), Input('select_stocks_interval', 'value'), Input('refresh_interval', 'n_intervals')])
def create_twitter_content_table(ticker_symbol, interval, refresh_interval):
    if ticker_symbol is not None:
        data = get_twitter_content(ticker_symbol, interval)
        if data is not None:
            data = data.sort_values(by=['date'], ascending=False)
            data['Tweet'] = data.apply(lambda row: f"[{row['tweet']}]({row['link']})", axis=1)
            data.rename(columns={'date': 'Date', 'sentiment': 'Sentiment'}, inplace=True)
            data = data[['Date', 'Tweet', 'Sentiment']]
            columns = [
                {'name': 'Date', 'id': 'Date'},
                {'name': 'Tweet', 'id': 'Tweet', 'presentation': 'markdown'},
                {'name': 'Sentiment', 'id': 'Sentiment'}
            ]
            return create_table(data=data, columns=columns, title=f'{ticker_symbol} Tweets')
        else:
            return create_alert('Pending data')
    else:
        return create_alert('Pending input for Ticker Symbol')

@app.callback(Output('get_stock_news_content', 'children'), 
[Input('input_stocks_ticker_symbol', 'value'), Input('select_stocks_interval', 'value'), Input('refresh_interval', 'n_intervals')])
def create_stock_news_content_table(ticker_symbol, interval, refresh_interval):
    if ticker_symbol is not None:
        data = get_stock_news_content(ticker_symbol, interval)
        if data is not None:
            data = data.sort_values(by=['date'], ascending=False)
            data['Headline'] = data.apply(lambda row: f"[{row['title']}]({row['link']})", axis=1)
            data.rename(columns={'date': 'Date', 'sentiment': 'Sentiment'}, inplace=True)
            data = data[['Date', 'Headline', 'Sentiment']]
            columns = [
                {'name': 'Date', 'id': 'Date'},
                {'name': 'Headline', 'id': 'Headline', 'presentation': 'markdown'},
                {'name': 'Sentiment', 'id': 'Sentiment'}
            ]
            return create_table(data=data, columns=columns, title=f'{ticker_symbol} News')
        else:
            return create_alert('Pending data')
    else:
        return create_alert('Pending input for Ticker Symbol')

@app.callback(Output('get_tweets_time_series', 'children'), 
[Input('input_stocks_time_series_ticker_symbol', 'value'), Input('radioitems_time_series_granularity', 'value'), Input('refresh_interval', 'n_intervals')])
def create_tweets_time_series_line_chart(ticker_symbol, granularity, refresh_interval):
    if ticker_symbol is not None:
        data = get_tweets_time_series(ticker_symbol, granularity)
        if data is not None:
            granularity_title_mapping = {'hour': 'Hourly', 'day': 'Daily'}
            fig = plot_multi_line_chart(data=data, label_col_name='date', value_col_names=['Positive', 'Neutral', 'Negative'], title=f'{ticker_symbol} {granularity_title_mapping[granularity]} Tweets Count by Sentiment')
            return dcc.Graph(figure=fig)
        else:
            return create_alert('Pending data')
    else:
        return create_alert('Pending input for Ticker Symbol')

if __name__ == '__main__':
    app.run_server(dev_tools_hot_reload=True, debug=True)