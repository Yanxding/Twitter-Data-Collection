'''
Auther: Yanxiang Ding
Twitter Data Collection
'''

# import necessary packages
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from time import sleep
import json
import datetime
import tweepy
import math
import glob
import csv
import zipfile
import zlib
from tweepy import TweepError
import pandas as pd
import numpy as np
from dateutil import parser
import sys

# use webdriver to scrape tweet ids
def collect_primary_ids(user, start, end, driver_path):
    delay = 6  # time to wait on each page load before reading the page
    driver = webdriver.Chrome(driver_path)  # options are Chrome() Firefox() Safari()
    # don't mess with this stuff
    twitter_ids_filename = 'a.json'
    days = (end - start).days + 1
    id_selector = '.time a.tweet-timestamp'
    tweet_selector = 'li.js-stream-item'
    user = user.lower()
    ids = []

    for day in range(days):
        d1 = format_day(increment_day(start, 0))
        d2 = format_day(increment_day(start, 1))
        url = form_url(d1, d2)
        print(url)
        print(d1)
        driver.get(url)
        sleep(delay)
        try:
            found_tweets = driver.find_elements_by_css_selector(tweet_selector)
            increment = 10
            while len(found_tweets) >= increment:
                print('scrolling down to load more tweets')
                driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
                sleep(delay)
                found_tweets = driver.find_elements_by_css_selector(tweet_selector)
                increment += 10
            print('{} tweets found, {} total'.format(len(found_tweets), len(ids)))
            for tweet in found_tweets:
                try:
                    id = tweet.find_element_by_css_selector(id_selector).get_attribute('href').split('/')[-1]
                    ids.append(id)
                except StaleElementReferenceException as e:
                    print('lost element reference', tweet)
        except NoSuchElementException:
            print('no tweets on this day')
        start = increment_day(start, 1)
    print('all done here')
    driver.close()
    return ids

# format search date
def format_day(date):
    day = '0' + str(date.day) if len(str(date.day)) == 1 else str(date.day)
    month = '0' + str(date.month) if len(str(date.month)) == 1 else str(date.month)
    year = str(date.year)
    return '-'.join([year, month, day])

# format url for webdriver
def form_url(since, until):
    p = 'https://twitter.com/search?f=tweets&vertical=default&q=%40' + user + '%20since%3A' + since + '%20until%3A' + until + '&src=typd'
    return p

# date increment
def increment_day(date, i):
    return date + datetime.timedelta(days=i)

# identify if it is a retweet
def is_retweet(entry):
    return 'retweeted_status' in entry.keys()

# get source of the tweet
def get_source(entry):
    if '<' in entry["source"]:
        return entry["source"].split('>')[1].split('<')[0]
    else:
        return entry["source"]

# get tweet content using API
def get_content(user, ids, api):
    user = user.lower()
    output_file = '{}.json'.format(user)
    compression = zipfile.ZIP_DEFLATED
    print('total ids: {}'.format(len(ids)))
    
    all_data = []
    start = 0
    end = 100
    limit = len(ids)
    i = math.ceil(limit / 100)
    counter = 1
    for item in ids:
        if counter % 200 == 0:
            print(str(counter)+'out of'+str(len(ids)))
        sleep(4)
        try:
            tweet = api.get_status(item,tweet_mode= 'extended')
            all_data.append(dict(tweet._json))
            counter += 1
        except:
            continue
    print('metadata collection complete')
    
    results = []
    for entry in all_data:
        t = [
            entry["id_str"],
            parser.parse(entry["created_at"]),
            entry["full_text"],
            entry["user"]["id_str"],
            entry["user"]["screen_name"],
            entry["in_reply_to_status_id_str"],
            entry["in_reply_to_user_id_str"],
            entry["in_reply_to_screen_name"],
            entry["retweet_count"],
            entry["favorite_count"],
            entry["user"]["followers_count"],
            entry["user"]["location"],
            get_source(entry),
            is_retweet(entry)
            ]
        results.append(t)
    cln = ['tweet_id','time','text','auther_id','auther_name','reply_to_id','reply_to_user_id','reply_to_user_name','retweet_ct',
           'favorite_ct','follower_ct','location','source','is_retweet']
    df = pd.DataFrame(results, columns = cln)

    print('creating master json file')
    with open(output_file, 'w') as outfile:
        json.dump(all_data, outfile)
    print('creating ziped master json file')
    zf = zipfile.ZipFile('{}.zip'.format(user), mode='w')
    zf.write(output_file, compress_type=compression)
    zf.close()
    return df

# organize collected tweets and collect responses
def tweet_grouping(df, api):
    df = df.sort_values(by='time',ascending=False)
    cln = ['tweet_id','time','text','auther_id','auther_name','reply_to_id','reply_to_user_id','reply_to_user_name','retweet_ct',
           'favorite_ct','follower_ct','location','source','is_retweet','key']
    interaction = pd.DataFrame(columns = cln)
    
    key = 1
    count = 1
    df_id = df['tweet_id'].tolist()
    tweet_id_list = []
    
    for i in range(len(df)):
        m = []
        message_num = 1
        current = df.iloc[i].tolist()
        if current[0] in tweet_id_list:
            continue
        if current[-10] in tweet_id_list:
            k = interaction['key'].loc[interaction['tweet_id']==current[-10]]
            current.append(k)
            m.append(current)
            tweet_id_list.append(current[0])
            continue
        current.append(key)
        m.append(current)
        tweet_id_list.append(current[0])
        search_id = current[-10]
        
        while search_id != None:
            if search_id in df_id:
                current = list(df.loc[df['tweet_id']==search_id].iloc[0])
                current.append(key)
                m.append(current)
                tweet_id_list.append(current[0])
                search_id = current[-10]
            else:
                try:
                    tweet = api.get_status(search_id,tweet_mode= 'extended')
                    current = [tweet.id_str, tweet.created_at, tweet.full_text, tweet.user.id_str, tweet.user.screen_name, tweet.in_reply_to_status_id_str,
                               tweet.in_reply_to_user_id_str, tweet.in_reply_to_screen_name, tweet.retweet_count, tweet.favorite_count, 
                               tweet.user.followers_count, tweet.user.location, get_source(dict(tweet._json)),is_retweet(dict(tweet._json)), key]
                    m.append(current)
                    tweet_id_list.append(current[0])
                    search_id = current[-10]
                    count += 1
                except tweepy.TweepError as e: 
                    search_id = None
        record = pd.DataFrame(m,columns = cln)
        interaction = interaction.append(record, ignore_index=True)
        key += 1
        if (count % 200) == 0:
            print('take a break')
            sleep(900)
    return interaction

# recursively collect responses to the tweets
# tracking and pool is used to improve efficiency of searching with cost of memory
def form_url_other_response(user, since, until):
    p1 = 'https://twitter.com/search?f=tweets&vertical=default&q=to%3A'
    p2 =  user + '%20since%3A' + since + '%20until%3A' + until + '&src=typd'
    return p1+p2

def other_response(t, pool, tracking, api, driver_path, user_name):
    time = str(t['time'])
    yy = int(time[0:4])
    mm = int(time[5:7])
    dd = int(time[8:10])
    
    start = datetime.datetime(yy, mm, dd)  # year, month, day
    delay = 5  # time to wait on each page load before reading the page
    driver = webdriver.Chrome(driver_path)
    
    days = 3
    id_selector = '.time a.tweet-timestamp'
    tweet_selector = 'li.js-stream-item'
    user = t['auther_name']
    tweet_id = str(t['tweet_id'])
    ids = []
    count = 0
    
    if user == user_name:
        results = pd.DataFrame(columns=['tweet_id','time','text','auther_id','auther_name','reply_to_id','reply_to_user_id','reply_to_user_name',
                                        'retweet_ct','favorite_ct','follower_ct','location','source','is_retweet','key'])
        return results, pool, tracking
    
    if tweet_id in pool['reply_to_id']:
        for i in range(len(pool[pool['reply_to_id']==tweet_id])):
            result.append(pool[pool['reply_to_id']==tweet_id].iloc[i].tolist()+[t['key']])
    else:
        for day in range(days):
            d1 = format_day(increment_day(start, 0))
            d2 = format_day(increment_day(start, 1))
            query = str(d1) + user
            if query in tracking:
                continue
            url = form_url_other_response(user, d1, d2)
            print(url)
            print(d1)
            driver.get(url)
            sleep(delay)
            try:
                found_tweets = driver.find_elements_by_css_selector(tweet_selector)
                increment = 10
                while len(found_tweets) >= increment:
                    print('scrolling down to load more tweets')
                    driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
                    sleep(delay)
                    found_tweets = driver.find_elements_by_css_selector(tweet_selector)
                    increment += 10
                print('{} tweets found, {} total'.format(len(found_tweets), len(ids)))

                for tweet in found_tweets:
                    try:
                        id = tweet.find_element_by_css_selector(id_selector).get_attribute('href').split('/')[-1]
                        ids.append(id)
                    except StaleElementReferenceException as e:
                        print('lost element reference', tweet)
            except NoSuchElementException:
                print('no tweets on this day')
            
            tracking.append(query)
            start = increment_day(start, 1)
        print('all done here')
        driver.close()
    
        results = []
        sub_pool = []
        for status in ids:
            try:
                tweet = api.get_status(status, tweet_mode= 'extended')
            except tweepy.TweepError as e:
                continue
            r = [tweet.id_str,
                 tweet.created_at,
                 tweet.full_text,
                 tweet.user.id_str,
                 tweet.user.screen_name,
                 tweet.in_reply_to_status_id_str,
                 tweet.in_reply_to_user_id_str,
                 tweet.in_reply_to_screen_name,
                 tweet.retweet_count,
                 tweet.favorite_count,
                 tweet.user.followers_count,
                 tweet.user.location,
                 get_source(dict(tweet._json)),
                 is_retweet(dict(tweet._json))]
            sub_pool.append(r)
            if tweet.in_reply_to_status_id_str == tweet_id and (tweet.user.id != 22536055):
                results.append(r + [t['key']])
            count += 1
            if (count % 180) == 0:
                print('take a break')
                sleep(900)
        
    results = pd.DataFrame(results,columns=['tweet_id','time','text','auther_id','auther_name','reply_to_id','reply_to_user_id','reply_to_user_name',
                                            'retweet_ct','favorite_ct','follower_ct','location','source','is_retweet','key'])
    pool = pool.append(pd.DataFrame(sub_pool,columns=['tweet_id','time','text','auther_id','auther_name','reply_to_id','reply_to_user_id',
                                                      'reply_to_user_name','retweet_ct','favorite_ct','follower_ct','location','source','is_retweet']))
    pool = pool.reset_index(drop=True)
    if len(results) > 0:
        for j in range(len(results)):
            results_new, pool, tracking = other_response(results.iloc[j], pool, tracking, api, user_name)
            results = results.append(results_new)
    results = results.reset_index(drop=True)
    return results, pool, tracking

# collect retweets
def search_retweets(df, api):
    search_df = df.loc[(df['retweet_ct']>0) & (df['auther_name']!='AmericanAir')]
    results = []
    counter = 0
    length = len(search_df)
    for index, row in search_df.iterrows():
        key = row['key']
        try:
            retweets = api.retweets(row['tweet_id'], tweet_mode= 'extended')
            for tweet in retweets:
                r = [
                     tweet.id_str,
                     tweet.created_at,
                     tweet.full_text,
                     tweet.user.id_str,
                     tweet.user.screen_name,
                     tweet.in_reply_to_status_id_str,
                     tweet.in_reply_to_user_id_str,
                     tweet.in_reply_to_screen_name,
                     tweet.retweet_count,
                     tweet.favorite_count,
                     tweet.user.followers_count,
                     tweet.user.location,
                     get_source(dict(tweet._json)),
                     is_retweet(dict(tweet._json)),
                     key
                    ]
                results.append(r)
            counter += 1
            if (counter % 75) == 0:
                print("Take a break")
                if round(counter/length*100,0)%10 == 0:
                    print('Finish {}%'.format(round(counter/length*100,0)))
                sleep(900)
        except tweepy.TweepError as e:
            counter += 1
            if (counter % 75) == 0:
                print("Take a break")
                if round(counter/length*100,0)%10 == 0:
                    print('Finish {}%'.format(round(counter/length*100,0)))
                sleep(900)
            continue
    results = pd.DataFrame(results,columns=['tweet_id','time','text','auther_id','auther_name','reply_to_id','reply_to_user_id','reply_to_user_name',
                                            'retweet_ct','favorite_ct','follower_ct','location','source','is_retweet','key'])
    return results

# main function
def main(user, start, end):
    auth = tweepy.OAuthHandler('consumer_token', 'consumer_secret')
    auth.set_access_token('key', 'secret')
    driver_path = 'path to the webdriver'
    api = tweepy.API(auth)
    
    ids = collect_primary_ids(user, start, end, driver_path)                                                      # collect primary tweet ids
    primary_tweet = get_content(user, ids, api)                                                      # collect primary tweets
    interaction = tweet_grouping(primary_tweet, api)                                                 # collect primary full interaction data
    interaction = interaction.drop_duplicates(subset='tweet_id').reset_index(drop=True)              # remove duplicate records
    
    other = pd.DataFrame()                                                                           # collect others responses
    pool = pd.DataFrame(columns=['tweet_id','time','text','auther_id','auther_name','reply_to_id','reply_to_user_id','reply_to_user_name','retweet_ct',
                                 'favorite_ct','follower_ct','location','source','is_retweet'])
    tracking = []
    for i in range(len(interaction)):
        other_row, pool, tracking = other_response(row, pool, tracking, api, driver_path, user)
        other = other.append(other_row)
    
    data = interaction.append(other).drop_duplicates(subset='tweet_id').reset_index(drop=True)      # combine data
    retweet = search_retweets(data, api)                                                            # collect retweets
    data = data.append(retweet).drop_duplicates(subset='tweet_id').reset_index(drop=True)           # combine data
    return data

if __name__ == "__main__":
    user = 'target user'
    start = 'start date'
    end = 'end date'
    data = main()
