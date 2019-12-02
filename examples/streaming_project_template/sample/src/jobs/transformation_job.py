import json

# Our filter function:
def filter_tweets(tweet):
    json_tweet = json.loads(tweet)
    print("%"*40)
    print(json_tweet.get('lang'))
    print("&"*40)
    if json_tweet.get('lang'):  # When the lang key was not present it caused issues
        if json_tweet['lang'] == 'en':
            return True  # filter() requires a Boolean value
    return False



