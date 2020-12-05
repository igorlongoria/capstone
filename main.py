import os
import tweepy
from pprint import pprint
import json
from collections import Counter
import sqlalchemy
import pymysql

# Fetch the secrets from our virtual environment variables
CONSUMER_KEY = os.environ['TWITTER_CONSUMER_KEY']
CONSUMER_SECRET = os.environ['TWITTER_CONSUMER_SECRET']
ACCESS_TOKEN = os.environ['TWITTER_ACCESS_TOKEN']
ACCESS_SECRET = os.environ['TWITTER_ACCESS_SECRET']

# Authenticate to the service we're accessing
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

# Create the connection
api = tweepy.API(auth, wait_on_rate_limit=True,
                 wait_on_rate_limit_notify=True)

# Request tweet data from Twitter API.
result_set = []
for i in tweepy.Cursor(api.search, q="red sox", lang="en", tweet_mode="extended", since=2020-6-29).items(50):
    result_set.append(i._json)


# Write tweet data to a json file.
with open('red_sox_tweets.json', 'w') as f:
    json.dump(result_set, f)

# Read Tweet data from json file.
with open('red_sox_tweets.json') as f:
    result_set = json.load(f)

# Create a list of users.
user_list = []
for i in result_set:
    user_list.append(i["user"])


# Create a list of user names.
user = []
for u in user_list:
    user.append(u["screen_name"])

# Create a list of Tweet texts.
tweet_list = [i["full_text"] for i in result_set]

# Write a text file with tweets only from the tweet data.
with open('tweet_only.txt', 'w') as t:
    t.write('\n'.join(tweet_list))

# Create a list of tweet creation.
date_of_tweet = [i["created_at"] for i in result_set]

# Find average number of followers
def average_number_of_followers(result_set):
    user_list = [i["user"] for i in result_set]
    followers_count = []
    total_followers = 0
    for j in user_list:
        followers_count.append(j["followers_count"])
    for n in followers_count:
        total_followers += n
    return total_followers/len(user_list)

# Find average length of tweets in words.
def average_length_of_tweets_words(tweet_list):
    tweet_words = []
    for item in tweet_list:
        words = item.split()
        tweet_words.append(len(words))

    avg_words = sum(tweet_words) / len(tweet_words)
    return avg_words


# Find average length of tweets in characters.
def average_length_of_tweets_characters(tweet_list):
    total_char = 0
    for item in tweet_list:
        total_char += len(item)
    average_tweet_character = total_char / len(tweet_list)
    return average_tweet_character

# Find the percentage of tweets with hashtags.
def percentage_of_hashtag(tweet_list):

    total_hash = 0
    for i in tweet_list:
        for char in i:
            if char == "#":
                total_hash += 1
                break
    percentage = (total_hash/len(tweet_list) * 100)
    return percentage

# Find percentage of tweets with an @ sign.
def percentage_of_at(tweet_list):
    total_at = 0
    for i in tweet_list:
        for char in i:
            if char == "@":
                total_at += 1
                break
    percentage = (total_at / len(tweet_list) * 100)
    return percentage

# Find the 100 most common words in the tweet text.
def most_common_words(tweet_list):
    word_list = []
    for i in tweet_list:
        text = i.split()
        for word in text:
            word_list.append(word)
    word_counter = Counter(word_list)
    most_occur = word_counter.most_common(100)
    return  most_occur

# Find the 100 most common symbols in the tweet text.
def most_common_symbols(tweet_list):
    list_of_char = []
    for i in tweet_list:
        i = i.replace(" ", "")
        i = i.replace("\n", "")
        for j in i:
            if j.isalpha() == False and j.isdigit() == False:
                list_of_char.append(j)
    symbol_counter = Counter(list_of_char)
    most_occur = symbol_counter.most_common(100)
    return (most_occur)

# Find the percentage of tweets that use punctuation.
def percentage_of_tweets_with_punctuation(tweet_list):
    total_p = 0
    for i in tweet_list:
        for char in i:
            if char == "," or char == '.' or char ==';' or char == "!" or char == '?':
                total_p += 1
                break
    percentage = (total_p / len(tweet_list) * 100)
    return (percentage)

# Find the shortest word in a tweet.
def shortest_word(tweet_list):
    shortest_word = "         "
    for word in tweet_list:
        x = word.split()
        for j in x:
            if len(j) < len(shortest_word):
                shortest_word = j
    return f"The shortest word is {shortest_word}"

# Find the user with most tweets.
def user_with_most_tweets(user_list):
    tweet_count_list = []
    user_name_list = []
    most_tweets = 0
    user_name = " "
    for i in user_list:
        user_name_list.append(i["screen_name"])
        tweet_count_list.append(i["statuses_count"])
    for (tweets, user) in zip(tweet_count_list, user_name_list):
        if tweets > most_tweets:
            most_tweets = tweets
            user_name = user

    return  f"{user_name} is the user with most tweets at {most_tweets}"

# Find the average number of tweets in the tweet list.
def average_number_tweets(user_list):
    tweet_count_list = []
    total_tweets = 0
    tweets = len(tweet_list)
    for i in user_list:
        tweet_count_list.append(i["statuses_count"])
    for n in tweet_count_list:
        total_tweets += n
    avg_num_tweets = total_tweets / tweets
    return  avg_num_tweets

# Find the hour with the greatest number of tweets.
def most_common_hour(result_set):
    time_created = [i["created_at"] for i in result_set]
    time_list = []
    for i in time_created:
        x = i.split()
        time_list.append(x[3])
    hour_list = []
    for j in time_list:
        hours, minutes, seconds = j.split(":")
        hour_list.append(hours)
    hour_counter = Counter(hour_list)
    most_common_hour = hour_counter.most_common(1)
    return  most_common_hour

# Establish connection to mysql database.
engine = sqlalchemy.create_engine('mysql+pymysql://root:PASSWORD@localhost/Capstone')
connection = engine.connect()
metadata = sqlalchemy.MetaData()

# Create a table to insert tweeter data.
twitter_data = sqlalchemy.Table('twitter_data', metadata,
                                   sqlalchemy.Column('created_at', sqlalchemy.String(100), nullable=False),
                                   sqlalchemy.Column('id', sqlalchemy.String(50), nullable=False),
                                   sqlalchemy.Column('full_text', sqlalchemy.String(500), nullable=False),
                                   sqlalchemy.Column('user_name', sqlalchemy.String(50), nullable=False),
                                   sqlalchemy.Column('followers_count', sqlalchemy.String(50), nullable=False),
                                   sqlalchemy.Column('statuses_count', sqlalchemy.String(50), nullable=False))

metadata.create_all(engine)

# Insert data obtained from tweeter API into database.
twitter_data_to_insert = []
for (ca, i, text, us, fc, sc) in zip(result_set, result_set, result_set, user_list, user_list, user_list):
    tweet_info = {'created_at':ca['created_at'],
                  'id':i['id'],
                  'full_text':text['full_text'],
                  'user_name': us['screen_name'],
                  'followers_count': fc['followers_count'],
                  'statuses_count': sc['statuses_count']
                  }
    twitter_data_to_insert.append(tweet_info)
query = sqlalchemy.insert(twitter_data)
result_proxy = connection.execute(query, twitter_data_to_insert)

# Create a table to insert the results of the Tweeter data statistics.
capstone_results = sqlalchemy.Table('capstone_results', metadata,
                                   sqlalchemy.Column('avg_num_followers', sqlalchemy.Integer(), nullable=False),
                                   sqlalchemy.Column('avg_length_tweets', sqlalchemy.Integer(), nullable=False),
                                   sqlalchemy.Column('avg_length_char', sqlalchemy.Integer(), nullable=False),
                                   sqlalchemy.Column('percent_hashtag', sqlalchemy.Integer(), nullable=False),
                                   sqlalchemy.Column('percent_at', sqlalchemy.Integer(), nullable=False),
                                   sqlalchemy.Column('most_common_words', sqlalchemy.JSON(100), nullable=False),
                                   sqlalchemy.Column('most_common_symbol', sqlalchemy.JSON(100), nullable=False),
                                   sqlalchemy.Column('percent_with_punct', sqlalchemy.Integer(), nullable=False),
                                   sqlalchemy.Column('shortest_word', sqlalchemy.JSON(100), nullable=False),
                                   sqlalchemy.Column('user_most_tweets', sqlalchemy.JSON(100), nullable=False),
                                   sqlalchemy.Column('avg_num_tweet', sqlalchemy.Integer(), nullable=False),
                                   sqlalchemy.Column('most_common_hour', sqlalchemy.JSON(50), nullable=False))

metadata.create_all(engine)

# Insert the Tweeter data statistics into database.
query_2 = sqlalchemy.insert(capstone_results).values(avg_num_followers=average_number_of_followers(result_set),
                                                     avg_length_tweets=average_length_of_tweets_words(tweet_list),
                                                     avg_length_char=average_length_of_tweets_characters(tweet_list),
                                                     percent_hashtag=percentage_of_hashtag(tweet_list),
                                                     percent_at=percentage_of_at(tweet_list),
                                                     most_common_words=most_common_words(tweet_list),
                                                     most_common_symbol=most_common_symbols(tweet_list),
                                                     percent_with_punct=percentage_of_tweets_with_punctuation(tweet_list),
                                                     shortest_word=shortest_word(tweet_list),
                                                     user_most_tweets=user_with_most_tweets(user_list),
                                                     avg_num_tweet=average_number_tweets(user_list),
                                                     most_common_hour=most_common_hour(result_set))
result_proxy_2 = connection.execute(query_2)

# Extract the tweeter data from the database.
twitter_data = sqlalchemy.Table('twitter_data', metadata, autoload=True, autoload_with=engine)

query_for_twitter_data = sqlalchemy.select([twitter_data])
result_proxy_3 = connection.execute(query_for_twitter_data)
result_set_twitter_data = result_proxy_3.fetchall()
pprint(result_set_twitter_data)

# Extract the results from the database.
capstone_results = sqlalchemy.Table('capstone_results', metadata, autoload=True, autoload_with=engine)

query_to_select = sqlalchemy.select([capstone_results])
result_proxy_4 = connection.execute(query_to_select)
result_set_capstone = result_proxy_4.fetchall()
pprint(result_set_capstone)

