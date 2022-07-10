import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import time
import preprocessor
from langdetect import detect
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from sentiment_classifier import SentimentPipeline
from common import KAFKA_BOOTSTRAP_SERVER, TWITTER_RAW_TOPIC, TWITTER_SENTIMENT_TOPIC, TWITTER_CHECKPOINT, NEWS_MARKET_RAW_TOPIC, NEWS_MARKET_SENTIMENT_TOPIC, NEWS_MARKET_CHECKPOINT, NEWS_STOCK_RAW_TOPIC, NEWS_STOCK_SENTIMENT_TOPIC, NEWS_STOCK_CHECKPOINT

# QUERY_INTERVAL_SECONDS = 5

preprocessor.set_options(preprocessor.OPT.URL, preprocessor.OPT.MENTION, preprocessor.OPT.HASHTAG, preprocessor.OPT.EMOJI, preprocessor.OPT.SMILEY)

def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName('spark-structured-streaming') \
        .master('local[2]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1') \
        .config('spark.streaming.concurrentJobs', '2') \
        .config('spark.driver.memory', '600m') \
        .getOrCreate()
        # .config('spark.cores.max', 1) \
        # .config('spark.executor.memory', '1g') \
    spark.sparkContext.addPyFile('/home/jieying/scripts/sentiment_classifier.py')
    spark.sparkContext.setLogLevel('ERROR')
    return spark

def read_kafka_stream(spark, kafka_topic, stream_schema):
    stream_df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVER) \
        .option('subscribe', kafka_topic) \
        .option('startingOffsets', 'earliest') \
        .load() \
        .selectExpr('CAST(value AS STRING)') \
        .select(from_json(col('value'), stream_schema) \
        .alias('data'))
    stream_df.printSchema()
    return stream_df.select('data.*')

def write_kafka_stream(stream_df, kafka_topic, checkpoint_path):
    # console_query = stream_df \
    #     .writeStream \
    #     .format('console') \
    #     .trigger(once=True) \
    #     .start()

    output_query = stream_df \
        .selectExpr('to_json(struct(*)) AS value') \
        .writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVER) \
        .option('topic', kafka_topic) \
        .option('checkpointLocation', checkpoint_path) \
        .trigger(once=True) \
        .start()
    
    # .trigger(continuous=f'{QUERY_INTERVAL_SECONDS} seconds') \
    return output_query

def preprocess_tweet(text):
    text = preprocessor.clean(text)
    text = re.sub('\$\w+','', text)
    text = re.sub(' +', ' ', text).strip()
    return text

def detect_language(text):
    try:
        language = detect(text)
    except:
        language = 'unknown'
    return language

def process_twitter_kafka_stream(spark, classify_udf):
    print(f'Reading stream from "{TWITTER_RAW_TOPIC}" topic')
    stream_schema = StructType() \
        .add('date', StringType()) \
        .add('ticker_symbol', StringType()) \
        .add('link', StringType()) \
        .add('tweet', StringType())
    stream_df = read_kafka_stream(spark, TWITTER_RAW_TOPIC, stream_schema)

    print(f'Processing tweets')

    preprocess_udf = udf(preprocess_tweet, StringType())
    preprocessed_df = stream_df.select('date', 'ticker_symbol', 'link', 'tweet', preprocess_udf('tweet').alias('tweet_preprocessed'))
    
    detect_lang_udf = udf(detect_language, StringType())
    detect_lang_df = preprocessed_df.select('date', 'ticker_symbol', 'link', 'tweet', 'tweet_preprocessed', detect_lang_udf('tweet_preprocessed').alias('language'))

    processed_df = detect_lang_df.select('date', 'ticker_symbol', 'link', 'tweet', 'tweet_preprocessed', 'language', classify_udf('tweet_preprocessed').alias('sentiment'))

    print(f'Writing processed tweets to "{TWITTER_SENTIMENT_TOPIC}" topic')
    output_query = write_kafka_stream(processed_df, TWITTER_SENTIMENT_TOPIC, TWITTER_CHECKPOINT)
    output_query.awaitTermination()
    
    # while True:
    #     # print(output_query.status, end='\r', flush=True)
    #     if (output_query.status['message'] == 'Waiting for data to arrive') and not (output_query.status['isDataAvailable']):
    #         break

    # print(output_query.status)
    # time.sleep(5)
    # output_query.stop()
    # print(output_query.status)

def process_market_news_kafka_stream(spark, classify_udf):
    print(f'Reading stream from "{NEWS_MARKET_RAW_TOPIC}" topic')
    stream_schema = StructType() \
        .add('date', StringType()) \
        .add('link', StringType()) \
        .add('title', StringType())
    stream_df = read_kafka_stream(spark, NEWS_MARKET_RAW_TOPIC, stream_schema)

    print(f'Processing market news')
    processed_df = stream_df.select('date', 'link', 'title', classify_udf('title').alias('sentiment'))

    print(f'Writing processed market news to "{NEWS_MARKET_SENTIMENT_TOPIC}" topic')
    output_query = write_kafka_stream(processed_df, NEWS_MARKET_SENTIMENT_TOPIC, NEWS_MARKET_CHECKPOINT)
    output_query.awaitTermination()
    
    # while True:
    #     # print(output_query.status, end='\r', flush=True)
    #     if (output_query.status['message'] == 'Waiting for data to arrive') and not (output_query.status['isDataAvailable']):
    #         break

    # print(output_query.status)
    # time.sleep(5)
    # output_query.stop()
    # print(output_query.status)

def process_stock_news_kafka_stream(spark, classify_udf):
    print(f'Reading stream from "{NEWS_STOCK_RAW_TOPIC}" topic')
    stream_schema = StructType() \
        .add('date', StringType()) \
        .add('ticker_symbol', StringType()) \
        .add('link', StringType()) \
        .add('title', StringType())
    stream_df = read_kafka_stream(spark, NEWS_STOCK_RAW_TOPIC, stream_schema)
    
    print(f'Processing stock news')
    processed_df = stream_df.select('date', 'ticker_symbol', 'link', 'title', classify_udf('title').alias('sentiment'))

    print(f'Writing processed stock news to "{NEWS_STOCK_SENTIMENT_TOPIC}" topic')
    output_query = write_kafka_stream(processed_df, NEWS_STOCK_SENTIMENT_TOPIC, NEWS_STOCK_CHECKPOINT)
    output_query.awaitTermination()
    
    # while True:
    #     # print(output_query.status, end='\r', flush=True)
    #     if (output_query.status['message'] == 'Waiting for data to arrive') and not (output_query.status['isDataAvailable']):
    #         break

    # print(output_query.status)
    # time.sleep(5)
    # output_query.stop()
    # print(output_query.status)

def main():
    spark = create_spark_session()
    
    sentiment_pipeline = SentimentPipeline()
    classify_udf = udf(sentiment_pipeline.classify, StringType())

    print('\n --Twitter--')
    process_twitter_kafka_stream(spark, classify_udf)
    print('\n --Market_News--')
    process_market_news_kafka_stream(spark, classify_udf)
    print('\n --Stock_News--')
    process_stock_news_kafka_stream(spark, classify_udf)

    # input('\nPress enter to terminate Spark session > ')
    spark.stop()

if __name__ == '__main__':
    main()