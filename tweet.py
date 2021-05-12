import tweepy
import secrets
import toml
import os 

TWITTER_API_KEY = os.getenv('TWITTER_API_KEY')
TWITTER_API_SECRET_KEY = os.getenv('TWITTER_API_SECRET_KEY')
TWITTER_TOKEN = os.getenv('TWITTER_TOKEN')
TWITTER_SECRET_TOKEN = os.getenv('TWITTER_SECRET_TOKEN')

# Deprecated. Function to tweet if we want.

auth = tweepy.OAuthHandler(TWITTER_API_KEY, TWITTER_API_SECRET_KEY)
auth.set_access_token(TWITTER_TOKEN, TWITTER_SECRET_TOKEN)

api = tweepy.API(auth)

config = toml.load('./config.toml')
for itemGroup, detail in config.items():
    # load image
    imagePath = "plots/"+itemGroup+".png"
    f = open("kpis/"+itemGroup+".txt", "r")
    status = f.read()
    # Send the tweet.
    api.update_with_media(imagePath, status)
