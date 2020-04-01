from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
from pymongo import MongoClient
import random
import re
import slack
from sqlalchemy import create_engine
from config import SLACK_TOKEN
import time

# Creating connections
CLIENT = MongoClient("mongodb")
DB = CLIENT.mongodb

DATABASE_UP = False

PG = create_engine('postgres://postgres:1234@postgresdb:5432/tweets')
PG.execute('''CREATE TABLE IF NOT EXISTS kungflu (
id BIGSERIAL,
text VARCHAR(1024),
sentiment NUMERIC
);
''')

client = slack.WebClient(token=SLACK_TOKEN)
# Creating python callables


def extract():
    """gets a random tweet"""
    tweets = list(DB.kungflu.find())
    if tweets:
        t = random.choice(tweets)
        logging.critical("random tweet: " + t['text'])
        return t


def transform(**context):
    # here we will insert the sentiment analysis results
    extract_connection = context['task_instance']
    tweet = extract_connection.xcom_pull(task_ids="extract")
    text = re.sub("'", "", tweet["text"])
    sentiment = 1
    logging.critical("sentiment" + str(sentiment))
    results = [text, sentiment]
    return results


def load(**context):
    exctract_connection = context["task_instance"]
    results = exctract_connection.xcom_pull(task_ids='transform')
    tweet, sentiment = results[0], results[1]
    PG.execute(f"""INSERT INTO kungflu (text, sentiment) VALUES ('''{tweet}''', {sentiment});""")
    logging.critical(f"tweet + sentiment written to PostGres")


def slackbot():
    prev_tweet = ''
    while True:
        # Do the slack post
        tweet_result = PG.execute('''SELECT kungflu."text" FROM kungflu ORDER BY id DESC LIMIT 1;''').fetchall()
        # logic to check if the tweet has already been posted, only post if its not already been posted
        if tweet_result != prev_tweet:
            prev_tweet = tweet_result
            response = client.chat_postMessage(channel='#kung_flu', text=f"Here is a tweet we scraped: {tweet_result}")
        #delay for one minute
        time.sleep(60)



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
t4 = PythonOperator(task_id='slackbot', python_callable=slackbot, dag=dag)


# setup dependencies
t1 >> t2 >> t3 >> t4
