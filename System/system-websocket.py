# System using the websocket

# Import packages and config
import tweepy as tw
import pandas as pd
import datetime as dt
import numpy as np
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
import websocket
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

messages_received = 0
messages_processed = 0

def on_error(ws, error):
    print("Error: " + error)

def on_close(ws):
    print("Websocket closed")

def on_open(ws):
    print("Websocket opened")

def on_message(ws, message):
    global tweet_queue
    global messages_received
    status = message

    messages_received = messages_received + 1

    tweet_queue.put(status)
        
def tweet_processing_thread():
    global tweet_queue
    
    while True:
        item = tweet_queue.get()
        process_tweet(item)
        tweet_queue.task_done()
        
# Main thread
def get_tweet_stream():
    ws2 = websocket.WebSocketApp("insert-websocket-url", on_open = on_open, on_message = on_message ,on_error = on_error, on_close = on_close)
    
    ws2.run_forever()

def process_tweet(status):
    global df
    global messages_processed
    global windows
    global events

    initial_tweet = json.loads(status)
    
    messages_processed = messages_processed + 1

    # Get the text of the tweet 
    pre_text = initial_tweet.get("pre_text_no_hashtag")

    urls = initial_tweet.get("pre_urls")

    # Set title to None, otherwise you get an error if you don't have a title
    title = None

    # Add the title of the URL to the text variable (if there is a url in the tweet)
    if(urls != None) & (urls != '[]'):
        
        # Make sure urls is a list
        urls = urls.strip('][').split(', ')
        
        for url in urls:
            
            url = url.replace('"', '')
            url = url.replace("'", "")
            
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

                if ("Pagina niet gevonden" in title) or ("Aanmelden bij Facebook" in title) or ("Not Found" in title) or ("Pagina bestaat niet meer" in title) or ("DPG Media Privacy Gate" in title) or ("404" in title) or ("Voordat je verdergaat naar YouTube" in title) or (title=='[]'):
                    title = None
                else:
                    title = clean_text(title, 'lose', 'string')
                    pre_text = pre_text + " " + str(title)
            except:
                pass

    created_at = pd.to_datetime(initial_tweet.get("created_at"))
    created_at = created_at + pd.Timedelta(hours=2)
    
    cleaned_tweet = {"created_at": created_at, "status_id" : initial_tweet.get("status_id"), "preprocessed_text_no_hashtag":pre_text, "text":initial_tweet.get("text"), "coordinates": initial_tweet.get("pre_coordinates"), "place":initial_tweet.get("pre_place"), "pre_media":initial_tweet.get("pre_media"), "urls":initial_tweet.get("pre_urls")}

    df = df.append(cleaned_tweet, ignore_index=True)
    
    AddtoTimewindow(cleaned_tweet)

    if messages_processed == 3515:
        df.to_csv("Output/5 minute window/df-5-minute-0125.csv")
        windows.to_csv("Output/5 minute windowindows-5-minute-0125.csv")
        events.to_csv("Output/5 minute windowevents-5-minute-0125.csv")
        print("Dataframes are created!")

def AddtoTimewindow(cleaned_tweet):
    global windows
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

# declarations
df = pd.DataFrame()
n_windows = 5
windows = pd.DataFrame(columns=["start_window", "tweet_ids", 'n_tweets'])
events = pd.DataFrame(columns=["start_window", "tweet_ids", 'n_tweets', 'is_incident', 'incident_related_tweets', 'first_incident_related_tweet_created_at'])

length_window = 5   # In minutes
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