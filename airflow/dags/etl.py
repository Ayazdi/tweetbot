import time
from datetime import datetime, timedelta
import random
import re
import logging
from config import SLACK_TOKEN

import pandas as pd
import slack
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
from sqlalchemy import create_engine
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

s = SentimentIntensityAnalyzer()

# Creating connections
CLIENT = MongoClient("mongodb")  # Mongodb
DB = CLIENT.mongodb
DATABASE_UP = False

PG = create_engine('postgres://postgres:1234@postgresdb:5432/tweets')  # PostGres
PG.execute('''CREATE TABLE IF NOT EXISTS kung_tweets (
id BIGSERIAL,
username VARCHAR(128),
text VARCHAR(2048),
date_created TIMESTAMPTZ,
followers INTEGER,
friends INTEGER,
negative NUMERIC,
positive NUMERIC,
neuteral NUMERIC
);
''')

client = slack.WebClient(token=SLACK_TOKEN)  # Slac


# Creating python callables
def extract():
    """gets a random tweet"""
    tweets = list(DB.kung_tweets.find())
    if tweets:
        t = random.choice(tweets)
        logging.critical("random tweet: " + t['text'])
        return t


def transform(**context):
    # here we will insert the sentiment analysis results
    extract_connection = context['task_instance']
    tweet = extract_connection.xcom_pull(task_ids="extract")
    if 'retweeted_status'in tweet and 'extended_tweet' in tweet['retweeted_status']:
        text = tweet['retweeted_status']['extended_tweet']["full_text"]
    elif 'retweeted_status'in tweet and 'text' in tweet['retweeted_status']:
        text = tweet['retweeted_status']["text"]
    elif "extended_tweet" in tweet:
        text = tweet['extended_tweet']["full_text"]
    else:
        text = tweet['text']
    text = re.sub(r"'", ' ', text)
    text = re.sub(r'@\S+|https?://\S+', '', text)
    username = tweet['user']['screen_name']
    date = str(pd.to_datetime(tweet["created_at"]))
    followers = tweet['user']['followers_count']
    friends = tweet['user']['friends_count']
    sentiment = s.polarity_scores(text)
    neg = sentiment["neg"]
    pos = sentiment["pos"]
    neu = sentiment["neu"]
    logging.critical("sentiment" + str(sentiment))
    results = [username, text, date, followers, friends, neg, pos, neu]
    return results


def load(**context):
    exctract_connection = context["task_instance"]
    results = exctract_connection.xcom_pull(task_ids='transform')
    PG.execute(f"""INSERT INTO kung_tweets (username, text, date_created, followers, friends, negative, positive, neuteral)
                VALUES ('''{results[0]}''', '''{results[1]}''', '{results[2]}', {results[3]}, {results[4]}, {results[5]}, {results[6]}, {results[7]});""")
    logging.critical(f"{results[1]} written to PostGres")


def slackbot(**context):
    exctract_connection = context["task_instance"]
    results = exctract_connection.xcom_pull(task_ids='transform')
    prev_tweet = ''
    if results[6] > 0.1:  # if the positive score is higher than 0.3
        tweet_result = results[1]  # text
        if tweet_result != prev_tweet:
            prev_tweet = tweet_result
            response = client.chat_postMessage(channel='#kung_flu', text=f"Here is a positive tweet we scraped: {tweet_result}")
        #delay for one minute
        time.sleep(20)



# define default arguments
default_args = {
                'owner': 'Amirali',
                'start_date': datetime(2020, 4, 1),
                # 'end_date':
                'email': ['amirali.yazdi@yahoo.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                "retries": 1,
                "retry_delay": timedelta(minutes=1)
}

# instantiate a DAG
dag = DAG('etl', description='', catchup=False, schedule_interval=timedelta(minutes=1), default_args=default_args)

# define task

t1 = PythonOperator(task_id='extract', python_callable=extract, dag=dag)
t2 = PythonOperator(task_id='transform', provide_context=True, python_callable=transform, dag=dag)
t3 = PythonOperator(task_id='load', provide_context=True, python_callable=load, dag=dag)
t4 = PythonOperator(task_id='slackbot', provide_context=True, python_callable=slackbot, dag=dag)


# setup dependencies
t1 >> t2 >> t3
t2 >> t4
