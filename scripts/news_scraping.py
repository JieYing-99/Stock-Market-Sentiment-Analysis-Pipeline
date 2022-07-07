from finvizfinance.news import News
from finvizfinance.quote import finvizfinance
from datetime import datetime
import pandas as pd

from kafka_producer import Producer
from common import *

pd.options.mode.chained_assignment = None

SCRAPE_NEWS_MINUTES_AGO = 10

def get_datetime_to_scrape():
    return (get_timestamp_minutes_before(timestamp=get_current_eastern_timestamp(), n_minutes=SCRAPE_NEWS_MINUTES_AGO)).strftime(TIMESTAMP_FORMAT)

def check_timestamp(timestamp):
    return timestamp.strftime(TIMESTAMP_FORMAT) >= get_datetime_to_scrape()

def get_standard_timestamp(timestamp):
    return datetime.strptime(get_current_eastern_timestamp().strftime('%Y-%m-%d') + ' ' + convert_12_hour_to_24_hour(timestamp) + ':00', TIMESTAMP_FORMAT)

def get_market_news(): # can only get today's news
    news = News()
    df = news.get_news()['news']
    df.columns= df.columns.str.lower()
    # filter for today's news
    df = df[df['date'].str.match('(1[0-2]|0?[1-9]):([0-5][0-9])?([AaPp][Mm])')]
    # convert date to standard timestamp format
    df['date'] = df['date'].apply(get_standard_timestamp)
    # filter for past n minutes' news
    df = df[df['date'].apply(check_timestamp)]
    if not df.empty:
        df['date'] = df['date'].astype(str)
    return df

def get_stock_news(ticker_symbol):
    stock = finvizfinance(ticker_symbol.lower())
    df = stock.ticker_news()
    df.columns= df.columns.str.lower()
    # filter for past n minutes' news
    df = df[df['date'].apply(check_timestamp)]
    if not df.empty:
        df['date'] = df['date'].astype(str)
        df['ticker_symbol'] = ticker_symbol
    return df

def main():
    news_producer = Producer()
    print(f' --- Scraping news published since {get_datetime_to_scrape()} ET ---')

    print(' --Market_News--')
    market_news = get_market_news()
    # print(market_news[['Date', 'Title']].head())
    count = len(market_news)
    print(f'>> Total number of market news scraped: {count}')
    write_scraped_count(NEWS_MARKET_DATA_SOURCE_NAME, count)
    if count > 0:
        print(f'Writing market news to Kafka "{NEWS_MARKET_RAW_TOPIC}" topic')
        market_news_list = market_news.to_dict(orient='records')
        for item in market_news_list:
            news_producer.send(NEWS_MARKET_RAW_TOPIC, item)
        news_producer.flush()

    print(' --Stock_News--')
    ticker_symbols = get_ticker_symbols()
    count = 0
    for ticker_symbol in ticker_symbols:
        print(ticker_symbol)
        stock_news = get_stock_news(ticker_symbol)
        # print(stock_news[['Date', 'Title']].head())
        print(f'Scraped {len(stock_news)} stock news')
        count += len(stock_news)
        if len(stock_news) > 0:
            print(f'Writing {ticker_symbol} stock news to Kafka "{NEWS_STOCK_RAW_TOPIC}" topic')
            stock_news_list = stock_news.to_dict(orient='records')
            for item in stock_news_list:
                news_producer.send(NEWS_STOCK_RAW_TOPIC, item)
            news_producer.flush()

    news_producer.close()
    print(f'>> Total number of stock news scraped: {count}')
    write_scraped_count(NEWS_STOCK_DATA_SOURCE_NAME, count)

if __name__ == '__main__':
    main()