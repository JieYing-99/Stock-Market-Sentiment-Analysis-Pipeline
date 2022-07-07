from pydruid.client import *
from pydruid.utils.aggregators import count
from pydruid.utils.filters import Dimension

from common import get_current_eastern_timestamp, get_timestamp_days_before, get_timestamp_hours_before, get_timestamp_minutes_before, convert_timestamp_to_ISO8601_format, convert_timestamp_to_standard_format, TWITTER_DATA_SOURCE_NAME, NEWS_MARKET_DATA_SOURCE_NAME, NEWS_STOCK_DATA_SOURCE_NAME, DRUID_BROKER_SOCKET

query = PyDruid(f'http://{DRUID_BROKER_SOCKET}', 'druid/v2')

duration_interval_mapping = {
    'last_15_mins': f'{convert_timestamp_to_ISO8601_format(get_timestamp_minutes_before(timestamp=get_current_eastern_timestamp(), n_minutes=15))}/pt15m',
    'last_30_mins': f'{convert_timestamp_to_ISO8601_format(get_timestamp_minutes_before(timestamp=get_current_eastern_timestamp(), n_minutes=30))}/pt30m',
    'last_1_hr': f'{convert_timestamp_to_ISO8601_format(get_timestamp_hours_before(timestamp=get_current_eastern_timestamp(), n_hours=1))}/pt1h',
    'last_24_hrs': f'{convert_timestamp_to_ISO8601_format(get_timestamp_hours_before(timestamp=get_current_eastern_timestamp(), n_hours=24))}/pt24h',
    'today': f'{get_current_eastern_timestamp().strftime("%Y-%m-%d")}/p1d',
    'last_3_days': f'{get_timestamp_days_before(timestamp=get_current_eastern_timestamp(), n_days=3).strftime("%Y-%m-%d")}/p3d',
}

granularity_interval_mapping = {
    'day': f'{get_timestamp_days_before(timestamp=get_current_eastern_timestamp(), n_days=6).strftime("%Y-%m-%d")}/p1w',
    'hour': f'{convert_timestamp_to_ISO8601_format(get_timestamp_hours_before(timestamp=get_current_eastern_timestamp(), n_hours=24))}/pt24h'
}

def get_twitter_top_trending_stocks(interval):
    # print(' --get_twitter_top_trending_stocks--')
    result = query.topn(
        datasource=TWITTER_DATA_SOURCE_NAME,
        granularity='all',
        intervals=duration_interval_mapping[interval],
        aggregations={'count': count('*')},
        dimension='ticker_symbol',
        metric='count',
        threshold=5
    )
    if result:
        if result[0]['result']:
            return result.export_pandas()[['ticker_symbol', 'count']]
    return None

def get_market_news_content(interval):
    # print(' --get_market_news_content--')
    result = query.scan(
        datasource=NEWS_MARKET_DATA_SOURCE_NAME,
        granularity='all',
        intervals=duration_interval_mapping[interval],
        columns=['date', 'title', 'link', 'sentiment']
    )
    if result:
        return result.export_pandas()
    else:
        return None

def get_stock_news_content(ticker_symbol, interval):
    # print(' --get_stock_news_content--')
    result = query.scan(
        datasource=NEWS_STOCK_DATA_SOURCE_NAME,
        granularity='all',
        intervals=duration_interval_mapping[interval],
        columns=['date', 'title', 'link', 'sentiment'],
        filter=Dimension('ticker_symbol') == ticker_symbol
    )
    if result:
        return result.export_pandas()
    else:
        return None

def get_twitter_content(ticker_symbol, interval):
    # print(' --get_twitter_content--')
    result = query.scan(
        datasource=TWITTER_DATA_SOURCE_NAME,
        granularity='all',
        intervals=duration_interval_mapping[interval],
        columns=['date', 'tweet', 'link', 'sentiment'],
        filter=Dimension('ticker_symbol') == ticker_symbol
    )
    if result:
        return result.export_pandas()
    else:
        return None

def get_market_news_sentiment_count(interval):
    # print(' --get_market_news_sentiment_count--')
    result = query.groupby(
        datasource=NEWS_MARKET_DATA_SOURCE_NAME,
        granularity='all',
        intervals=duration_interval_mapping[interval],
        dimensions=['sentiment'],
        aggregations={'count': count('*')}
    )
    if result:
        return result.export_pandas()[['sentiment', 'count']]
    else:
        return None

def get_stock_news_sentiment_count(ticker_symbol, interval):
    # print(' --get_stock_news_sentiment_count--')
    result = query.groupby(
        datasource=NEWS_STOCK_DATA_SOURCE_NAME,
        granularity='all',
        intervals=duration_interval_mapping[interval],
        dimensions=['sentiment'],
        filter=Dimension('ticker_symbol') == ticker_symbol,
        aggregations={'count': count('*')}
    )
    if result:
        return result.export_pandas()[['sentiment', 'count']]
    else:
        return None

def get_twitter_sentiment_count(ticker_symbol, interval):
    # print(' --get_twitter_sentiment_count--')
    result = query.groupby(
        datasource=TWITTER_DATA_SOURCE_NAME,
        granularity='all',
        intervals=duration_interval_mapping[interval],
        dimensions=['sentiment'],
        filter=Dimension('ticker_symbol') == ticker_symbol,
        aggregations={'count': count('*')}
    )
    if result:
        return result.export_pandas()[['sentiment', 'count']]
    else:
        return None

def get_tweets_time_series(ticker_symbol, granularity):
    # print(' --get_tweets_time_series--')
    result_list = []
    for sentiment in ['Positive', 'Neutral', 'Negative']:
        result = query.timeseries(
            datasource=TWITTER_DATA_SOURCE_NAME,
            granularity=granularity,
            intervals=granularity_interval_mapping[granularity],
            aggregations={sentiment: count('*')},
            filter=(Dimension('ticker_symbol') == ticker_symbol) & (Dimension('sentiment') == sentiment)
        )
        result_list.append(result.export_pandas())
    if result:
        merged_result = result_list[0].merge(result_list[1],on='timestamp').merge(result_list[2],on='timestamp')
        merged_result.rename(columns={'timestamp': 'date'}, inplace=True)
        merged_result['date'] = merged_result['date'].apply(convert_timestamp_to_standard_format)
        merged_result.sort_index(axis=1, inplace=True)
        return merged_result
    else:
        return None