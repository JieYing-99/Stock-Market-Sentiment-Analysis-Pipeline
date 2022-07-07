import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone
from dateutil import parser
import json

TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'

TIME_ZONE = 'America/New_York'

INPUT_PATH = '/home/jieying/input/stocks.csv'

KAFKA_BOOTSTRAP_SERVER = 'localhost:9092'

DRUID_OVERLORD_SOCKET = 'localhost:8180'
DRUID_BROKER_SOCKET = 'localhost:8280'

TWITTER_RAW_TOPIC = 'twitter-raw'
TWITTER_SENTIMENT_TOPIC = 'twitter-sentiment'
TWITTER_CHECKPOINT = '/home/jieying/output/checkpoints/twitter-checkpoint'
TWITTER_DATA_SOURCE_NAME = 'twitter'

NEWS_MARKET_RAW_TOPIC = 'news-market-raw'
NEWS_MARKET_SENTIMENT_TOPIC = 'news-market-sentiment'
NEWS_MARKET_CHECKPOINT = '/home/jieying/output/checkpoints/news-market-checkpoint'
NEWS_MARKET_DATA_SOURCE_NAME = 'news_market'

NEWS_STOCK_RAW_TOPIC = 'news-stock-raw'
NEWS_STOCK_SENTIMENT_TOPIC = 'news-stock-sentiment'
NEWS_STOCK_CHECKPOINT = '/home/jieying/output/checkpoints/news-stock-checkpoint'
NEWS_STOCK_DATA_SOURCE_NAME = 'news_stock'

SCRAPED_COUNT_PATH = '/home/jieying/output/scraped_count.json'

def get_ticker_symbols():
    df = pd.read_csv(INPUT_PATH)
    return df['ticker_symbol'].tolist()

def convert_time_to_eastern_time(timestamp):
    return datetime.strptime(timestamp, TIMESTAMP_FORMAT) - timedelta(hours=12)

def convert_time_to_local_time(timestamp):
    return datetime.strptime(timestamp, TIMESTAMP_FORMAT) + timedelta(hours=12)

def get_current_eastern_timestamp():
    return datetime.now(timezone(TIME_ZONE))

def get_timestamp_days_before(timestamp, n_days):
    return timestamp - timedelta(days=n_days)

def get_timestamp_hours_before(timestamp, n_hours):
    return timestamp - timedelta(hours=n_hours)

def get_timestamp_minutes_before(timestamp, n_minutes):
    return timestamp - timedelta(minutes=n_minutes)

def convert_12_hour_to_24_hour(time):
    original = datetime.strptime(time, '%I:%M%p')
    converted = datetime.strftime(original, "%H:%M")
    return converted

def convert_timestamp_to_ISO8601_format(timestamp):
    return timestamp.strftime('%Y-%m-%dT%H:%M:%S.000Z')

def convert_timestamp_to_standard_format(timestamp):
    return parser.parse(timestamp).strftime(TIMESTAMP_FORMAT)

def write_scraped_count(data_source, count):
    with open(SCRAPED_COUNT_PATH, 'r') as read_file:
        try:
            data = json.load(read_file)
        except Exception as e:
            data = {}
    data[data_source] = count
    with open(SCRAPED_COUNT_PATH, 'w') as write_file:
        json.dump(data, write_file)

def get_scraped_count(data_source):
    with open(SCRAPED_COUNT_PATH, 'r') as read_file:
        dict = json.load(read_file)
    return dict[data_source]