# System using the Twitter API

# Import packages and config
import tweepy as tw
import pandas as pd
import datetime as dt
import numpy as np
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer
import requests
import json
import pickle
from bs4 import BeautifulSoup
import queue
import threading
import time
import emoji

# Clean the text of the tweet
def clean_text(tweet, hashtag_text='keep', representation = 'string'):
    
    # Parameters
    # hashtag_text, default = 'keep'
        # 'keep' - keeps the hashtag text and only removes the '#' in the text
        # 'lose' - both removes the hashtag text and the '#' in the text
    # representation, default = 'string'
        # 'list' - returns a list of words
        # 'string' - returns a sentence in string format
    
    # Make the tweet lowercase
    tweet = tweet.lower()
    
    # Remove words with less than two characters
    tweet = re.sub(r'\b\w{1,2}\b', '', tweet)
    
    # Remove URLs
    tweet = remove_url(tweet)
    
    # Remove punctuations unless they are part of a digit (such as "5.000")
    tweet = re.sub(r'(?:(?<!\d)[.,;:??]|[.,;:??](?!\d))', '', tweet)
    
    # Remove emojis
    tweet = remove_emoji(tweet)
    
    if hashtag_text == 'keep':
        tweet = tweet.replace("#", "")
        tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)", "", tweet).split())
    else:
        # Remove hashtags and mentions
        tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|(#[A-Za-z0-9]+)", "", tweet).split())
    
    # Remove non-alphanumeric charachters, line breaks and tabs
    tweet = ' '.join(re.sub("([:/\!@#$%^&*()_+{}[\];\"\'|?<>~`\-\n\t?])", "", tweet).split())
    
    # Tokenize the tweet
    tweet = word_tokenize(tweet)
    
    # Remove stopwords
    tweet = [w for w in tweet if not w in stop_words]
    
    if representation == 'list':
        return tweet
    else:
        return listToString(tweet)
    
# Function to convert a list to a string
def listToString(s):  
    
    # initialize an empty string 
    str1 = " " 
    
    # return string   
    return (str1.join(s)) 

def remove_emoji(string):
    return emoji.get_emoji_regexp().sub(u'', string)

def remove_url(tweet_text):
    if has_url_regex(tweet_text): 
        url_regex_list = regex_url_extractor(tweet_text)
        for url in url_regex_list:
            tweet_text = tweet_text.replace(url, "")
    return tweet_text

def has_url_regex(tweet_text):
    return regex_url_extractor(tweet_text)

def regex_url_extractor(tweet_text):
    return re.findall('https?:\/\/(?:[-\w\/.]|(?:%[\da-fA-F]{2}))+', tweet_text)

def clean_field(field, type_field):
    if (field != "[]") and (field != None):                              # Check if the field has any value
        field_list = []
        for element in field:
            if (type_field == 'hashtag'):                   # Check if the field is hashtag field
                field_list.append(element["text"])
            elif (type_field == 'user_mentions'):        # Check if the field is a user_mentions field
                field_list.append(element["screen_name"])
            elif (type_field == 'media'):                # Check if the field is a media field
                field_list.append(element["media_url"])
            elif (type_field == 'urls'):
                field_list.append(element["expanded_url"])
        return field_list
    else:
        return None

# Function to get only the place if tweet.place is acquired as a whole
def clean_place(place):
    import pandas as pd
    
    if place != None:
        place = place.name
        
        return place
    
# Function to clean the coordinates columns
def clean_coordinates(coordinate):
    import ast
    
    # Check if the coordinates has any value
    if type(coordinate) == str:
        coordinate = ast.literal_eval(coordinate)  # Change the string to a dictionary (so we can get the necessary elements)
        i = 0
        for element in coordinate:
            if(i==0) :                             # This will skip the first element (not necessary)
                i = i+1
            else:
                return coordinate['coordinates']   # Return only the coordinates
    else:                                          # If the row is not a string it always is a nan, so we can set this to None
        return None

messages_received = 0
messages_processed = 0

class TweetListener(StreamListener):
    def on_status(self, status):
        global tweet_queue
        global messages_received

        tweet_queue.put(status)

        messages_received = messages_received + 1
        #print(messages_received)

        return True
    
    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False
        
def tweet_processing_thread():
    global tweet_queue
    
    while True:
        item = tweet_queue.get()
        process_tweet(item)
        tweet_queue.task_done()
        
# Main thread
def get_tweet_stream():
    # Set Twitter API Keys
    consumer_key= ''
    consumer_secret= ''
    access_token= ''
    access_token_secret= ''

    # Connect with Twitter API using Tweepy
    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tw.API(auth)
    
    l = TweetListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    stream.filter(track=['peter'], languages=['nl'])

def process_tweet(status):
    global messages_processed

    #initial_tweet = json.loads(status)
    
    messages_processed = messages_processed + 1
    #print(messages_processed)

    # If the tweet is truncated, use the full_text variable
    if status.truncated == True:
        text = status.extended_tweet['full_text']
    else:
        text = status.text
    
    # If the tweet has extended entities (e.g. multiple images), set the attribute
    if hasattr(status, 'extended_entities'):
        extended_entities = status.extended_entities
    else:
        extended_entities = None

    retweeted = False

    # If the tweet is a retweet, set multiple variables
    if 'RT' in text:
        retweeted = True

        # Check if the tweet has a retweeted_status
        if hasattr(status, 'retweeted_status'):
            retweeted_status = status.retweeted_status
            retweeted_status_id = status.retweeted_status.id
            retweeted_status_user_id = status.retweeted_status.user.id

            # If the tweet is a retweet, get the full text of the original tweet
            if status.retweeted_status.truncated == True:
                text = "RT @" + status.retweeted_status.user.screen_name + ": " + status.retweeted_status.extended_tweet['full_text']
            else:
                text = "RT @" + status.retweeted_status.user.screen_name + ": " + status.retweeted_status.text

        else:
            retweeted_status = None
            retweeted_status_id = None
            retweeted_status_user_id = None

    else:
        retweeted_status = None
        retweeted_status_id = None
        retweeted_status_user_id = None
    
    status_dict = {
        "created_at": status.created_at,
        "status_id" : status.id,
        "text" : text,
        "coordinates": status.coordinates,
        "source" : status.source,
        "in_reply_to_status_id" : status.in_reply_to_status_id,
        "in_reply_to_user_id" : status.in_reply_to_user_id,
        "in_reply_to_screen_name" : status.in_reply_to_screen_name,
        "retweeted" : retweeted,
        "retweeted_status": retweeted_status,
        "retweeted_status_id" : retweeted_status_id,
        "retweeted_status_user_id" : retweeted_status_user_id,
        "favorited" : status.favorited,
        "favorite_count" : status.favorite_count,
        "place" : status.place,
        "entities" : status.entities,
        "extended_entities" : extended_entities,
        "user": status.user,
        "user_id": status.user.id,
        "user_screen_name": status.user.screen_name,
        "lang" : status.lang
    }
    
    # Send the dictionary to cleanTweet() function to clean the tweet
    cleanTweet(status_dict)
    return True

def cleanTweet(status):
    global df
     
    # Add two hours to the current time, because NL is in UTC+2
    created_at = status.get("created_at") + dt.timedelta(hours=2)
    
    # Get the hashtags and clean them
    hashtags = status.get("entities").get("hashtags", None)
    hashtags = clean_field(hashtags, type_field='hashtags')
    
    # Get the coordinates and clean them
    coordinates = status.get("coordinates")
    coordinates = clean_coordinates(coordinates)
    
    # Get the user determined place and clean it
    place = status.get("place")
    place = clean_place(place)
    
    # If a tweet has an image, get the URL to the image
    if hasattr(status, 'extended_entities'):
        media = status.get("extended_entities").get("media")
        media = clean_field(media, type_field='media')
    else:
        media = None
    
    # Get URLs mentioned in a tweet
    urls = status.get("entities").get("urls")
    urls = clean_field(urls, type_field="urls")
    
    # Get the user mentions and clean it
    user_mentions = status.get("entities").get("user_mentions")
    user_mentions = clean_field(user_mentions, type_field='user_mentions')
    
    # Get the text of the tweet 
    text = status.get("text")
    
    # Set title to None, otherwise you get an error if you don't have a title
    title = None
    
    # Add the title of the URL to the text variable (if there is a url in the tweet)
    if(urls != None) & (urls != []):
        for url in urls:
            try:
                reqs = requests.get(url)

                # Get the HTML of the URL
                soup = BeautifulSoup(reqs.text, 'html.parser')

                # Get the title of the web page
                title= soup.find_all('title')

                # Get the title of the web page as a string
                if type(title) == str:
                    title = title.get_text()
                elif (title != None) and (title != []):
                    title = title[0].get_text()

                if ("Pagina niet gevonden" in title) or ("Aanmelden bij Facebook" in title) or ("Not Found" in title) or ("Pagina bestaat niet meer" in title):
                    title = None
                else:
                    text = text + " " + str(title)
            except:
                pass;
    
    # Clean the text of the tweet
    preprocessed_text = clean_text(text, 'keep', 'string')
    preprocessed_text_no_hashtag = clean_text(text, 'lose', 'string')
    preprocessed_text_tokenized = clean_text(text, 'keep', 'list')
    preprocessed_text_tokenized_no_hashtag = clean_text(text, 'lose', 'list')
        
    cleaned_tweet = {"created_at": created_at, "status_id" : status.get("status_id"), "user_id":status.get("user_id"),
        "user_screen_name":status.get("user_screen_name"), "retweeted" : status.get("retweeted"),
        "retweeted_status_id":status.get("retweeted_status_id"), "hashtags" : hashtags,
        "coordinates":coordinates, "place" : place, "text":text, "preprocessed_text":preprocessed_text,
        "preprocessed_text_no_hashtag":preprocessed_text_no_hashtag, "preprocessed_text_tokenized":preprocessed_text_tokenized,
        "preprocessed_text_tokenized_no_hashtag":preprocessed_text_tokenized_no_hashtag, "media":media,
        "urls":urls
    }
    
    df = df.append(cleaned_tweet, ignore_index=True)
        
    AddtoTimewindow(cleaned_tweet)

def AddtoTimewindow(cleaned_tweet):
    global windows
    global events
    global df
    global mean
    global mean_dev
    
    created_at = cleaned_tweet.get('created_at')
    tweet_id = cleaned_tweet.get('status_id')
    
    # If there is are now windows yet, create the first time window
    if windows.empty:
        start_window = created_at
        windows = windows.append({"start_window":start_window, "tweet_ids":[], 'n_tweets':0}, ignore_index=True)
    
    # Else get the start of the current time window (is always the latest time window)
    else:
        index_window = len(windows) - 1
        start_window = windows.loc[index_window, 'start_window']
    
    # While the created_at is lower than the start of this time window + length of the window, add tweet to time window
    # Else start a new time window
    while True:
                
        if created_at < (start_window + dt.timedelta(minutes=length_window)):
            index_row = windows[windows['start_window']==start_window].index.values.astype(int)[0]
            tweet_ids = windows.loc[index_row, 'tweet_ids']
            tweet_ids.append(tweet_id)
            windows["tweet_ids"][index_row] = tweet_ids
            windows["n_tweets"][index_row] = len(tweet_ids)
            
            break
        
        # When a tweet falls outside the current time window, detect if there is an event in the current time window
        # Then update the mean and mean_dev values
        # And eventually start a new window
        else:
            DetectEvent(cleaned_tweet, start_window)
            
            index_row = windows[windows['start_window']==start_window].index.values.astype(int)[0]
            tweets_in_window = windows.loc[index_row, 'n_tweets']

            if (len(windows) >= n_windows):
                mean, mean_dev = update(mean, mean_dev, tweets_in_window)
            
            start_window = start_window + dt.timedelta(minutes=length_window)
            windows = windows.append({"start_window":start_window, "tweet_ids":[], 'n_tweets':0}, ignore_index=True)
            
def DetectEvent(cleaned_tweet, start_window):
    global mean
    global mean_dev
    global events
    
    created_at = cleaned_tweet.get('created_at')
    tweet_id = cleaned_tweet.get('status_id')
    
    # Check if we already had enough time windows
    if len(windows) > n_windows:
            
        # Get the number of tweets of the current time window
        index_row = windows[windows['start_window']==start_window].index.values.astype(int)[0]
        n_tweets_current_window = windows["n_tweets"][index_row]
        
        # Get the number of tweets of the previous time window
        index_row_previous_window = index_row - 1
        n_tweets_previous_window = windows["n_tweets"][index_row_previous_window]

        # Check if the current number of tweets is more than 2 mean deviations from the current mean
        # And check if the number of tweets is increasing based on the previous time window
        if (n_tweets_current_window > (mean + (2*mean_dev))) and (n_tweets_current_window > n_tweets_previous_window):
            print("Event started!")

            # If there is a new event, get all the tweet_ids of the current time window
            tweet_ids = windows.loc[index_row, 'tweet_ids']

            # Add an event to the events dataframe
            events = events.append({"start_window":start_window, "tweet_ids":tweet_ids, "n_tweets":n_tweets_current_window, "is_incident":False, "incident_related_tweets":[], "first_incident_related_tweet_created_at":""}, ignore_index=True)
            
            # Detect if there is an incident
            DetectIncident(start_window)

def update(mean, mean_dev, n_tweets_current_window):
    global windows

    # Check if we already had enough time windows
    if len(windows) >= n_windows:
    
        # If the number of windows is equal to the number of n_windows, create the first values of mean and mean_dev
        if len(windows) == n_windows:
            newmean = np.mean(windows.loc[:(n_windows-1), 'n_tweets'])
            newmeandev = np.var(windows.loc[:(n_windows-1), 'n_tweets'])
        
        # If the number of windows is higher than the number of n_windows, update the values of mean and mean_dev
        else:
            diff = abs(mean-n_tweets_current_window)
            newmeandev = o*diff + (1-o) * mean_dev
            newmean = o*n_tweets_current_window + (1-o) * mean
        
    return newmean, newmeandev
        
def DetectIncident(start_window):
    
    # Get tweet ids of current time window
    index_row = windows[windows['start_window']==start_window].index.values.astype(int)[0]
    tweet_ids = windows["tweet_ids"][index_row]
    
    for tweet_id in tweet_ids:
        # Get the text of the tweet (without hashtag contents) and vectorize it
        index_tweet_id = df[df["status_id"]==tweet_id].index.values.astype(int)[0]
        text = df.loc[index_tweet_id, 'preprocessed_text_no_hashtag']
        text_arr = [text]
        text_tf = pd.DataFrame(vectorizer.transform(text_arr).todense(), columns=vectorizer.get_feature_names())

        # Predict if the tweet is incident_related
        pred = model.predict(text_tf)
            
        # If the tweet is Incident_Related
        if pred == [1]:

            # Get index of current event (this is always the last event)
            index_current_event = events.index[-1]

            # Add this tweet tweet to the incident_related tweets variable of the event
            incident_related_tweets = events.loc[index_current_event, 'incident_related_tweets']
            incident_related_tweets.append(tweet_id)
            events["incident_related_tweets"][index_current_event] = incident_related_tweets  

            # If there was no incident before, set the event to an incident and print when incident detected
            if events["is_incident"][index_current_event] == False:
                events["is_incident"][index_current_event] = True

                created_at_first_tweet = df.loc[index_tweet_id, 'created_at']
                print("There is an incident! Detected at: ", created_at_first_tweet)
                events["first_incident_related_tweet_created_at"][index_current_event] = created_at_first_tweet

tweet_queue = queue.Queue()
thread = threading.Thread(target=tweet_processing_thread)
thread.daemon = True
thread.start()

# Use Dutch stop words
stop_words = stopwords.words('dutch') + ["rt", "nan", "NaN"] 

# Declarations
df = pd.DataFrame()
n_windows = 5
windows = pd.DataFrame(columns=["start_window", "tweet_ids", 'n_tweets'])
events = pd.DataFrame(columns=["start_window", "tweet_ids", 'n_tweets', 'is_incident', 'incident_related_tweets', 'first_incident_related_tweet_created_at'])

length_window = 1   # In minutes
o = 0.125 #alpha value
mean = 0
mean_dev = 0

model = pickle.load(open('support-vector-machines-1.sav', 'rb'))
vectorizer = pickle.load(open('vectorizer_text_no_hashtag.pickle', 'rb'))

while True:
    try:
        get_tweet_stream()
    except KeyboardInterrupt:
        print ("Keyboard interrupt...")
        # Handle cleanup (save data, etc)
        break
        
    except:
        print("Error. Restarting...")
        time.sleep(5)
        pass