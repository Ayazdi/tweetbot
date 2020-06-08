# Tweetbot
A data pipeline to extract sarcastic tweets and post them on Slack

## Description
This Dockerized data pipeline extract tweets with given tags from Twitter and store them into **MongoDB** as JSON file. Then, with an **ETL** job maintained on **Airflow** transforms and loads the metadata to **PostGreSQL** database. Tweetbot pipeline also analyzes the sentiment of the tweets via VaderSentiment and checks if the they are sarcastic or not via deployed NLP model. Finally, it posts the sarcastic tweets to a Slack channel.
