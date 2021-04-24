import tweepy
import secrets
import toml

auth = tweepy.OAuthHandler(secrets.api_key, secrets.api_secret_key)
auth.set_access_token(secrets.token, secrets.secret_token)

api = tweepy.API(auth)

config = toml.load('./config.toml')
for itemGroup, detail in config.items():
    # load image
    imagePath = "plots/"+itemGroup+".png"
    f = open("kpis/"+itemGroup+".txt", "r")
    status = f.read()
    # Send the tweet.
    api.update_with_media(imagePath, status)