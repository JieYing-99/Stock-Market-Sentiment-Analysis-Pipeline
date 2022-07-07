import twint
from datetime import datetime
import pandas as pd

from kafka_producer import Producer
from common import *

# TWINT_OUTPUT_PATH = 'output/{}.json'
# TWINT_RESUME_PATH = 'output/resume.txt'
SCRAPE_TWEETS_MINUTES_AGO = 10

def get_search_terms():
    df = pd.read_csv(INPUT_PATH)
    # df['cashtag'] = '$' + df['ticker_symbol']
    # df['search_term'] = df[['cashtag', 'company', 'ceo']].apply(' OR '.join, axis=1)
    df['search_term'] = df['keywords'].str.replace('; ', ' OR ')
    return df['search_term'].tolist()

def get_datetime_to_scrape():
    return (get_timestamp_minutes_before(timestamp=datetime.now(), n_minutes=SCRAPE_TWEETS_MINUTES_AGO)).strftime(TIMESTAMP_FORMAT)

def scrape_tweets(search_term, datetime_to_scrape):
    config = twint.Config()
    config.Search = search_term
    config.Lang = 'en'
    config.Since = datetime_to_scrape
    config.Pandas = True
    # config.Store_object = True
    # config.Store_csv = True
    # config.Store_json = True
    # config.Output = TWINT_OUTPUT_PATH.format(ticker_symbol)
    config.Hide_output = True
    config.Count = True
    config.Popular_tweets = False
    config.Links = 'include' # 'include', 'exclude'
    config.Filter_retweets = False
    # config.Limit = 20
    # config.Resume = TWINT_RESUME_PATH
    twint.run.Search(config)

def main():
    tweet_producer = Producer()
    ticker_symbols = get_ticker_symbols()
    search_terms = get_search_terms()
    
    datetime_to_scrape = get_datetime_to_scrape()
    print(f' --- Scraping tweets created since {convert_time_to_eastern_time(datetime_to_scrape)} ET ---')

    count = 0
    for ticker_symbol, search_term in zip(ticker_symbols, search_terms):
        print(f'{ticker_symbol} --> {search_term}')
        scrape_tweets(search_term, datetime_to_scrape)
        # result_list = twint.output.tweets_list
        result_list = twint.storage.panda.Tweets_df.to_dict(orient='records')
        # result_json = json.dumps(result_list)
        # print(result_list)

        if result_list:
            print(f'Writing tweets to Kafka "{TWITTER_RAW_TOPIC}" topic')
            for item in result_list:
                item['ticker_symbol'] = ticker_symbol
                item['date'] = str(convert_time_to_eastern_time(item['date']))
                tweet_producer.send(TWITTER_RAW_TOPIC, item)

            tweet_producer.flush()

            count += len(twint.storage.panda.Tweets_df)
            # twint.storage.panda.clean()
            twint.storage.panda.Tweets_df = None
        
    tweet_producer.close()
    print(f'>> Total number of tweets scraped: {count}')
    write_scraped_count(TWITTER_DATA_SOURCE_NAME, count)

if __name__ == '__main__':
    main()