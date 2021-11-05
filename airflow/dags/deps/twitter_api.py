import os
import re
import tweepy
from tweepy import OAuthHandler

from dotenv import load_dotenv

load_dotenv()


class TwitterClient:
    """
    Generic Twitter client partially taken from
    https://www.geeksforgeeks.org/twitter-sentiment-analysis-using-python/
    """

    def __init__(self):
        """
        Class constructor or initialization method.
        """
        # keys and tokens from the Twitter Dev Console
        consumer_key = os.getenv("CONSUMER_KEY")
        consumer_secret = os.getenv("CONSUMER_SECRET")
        access_token = os.getenv("ACCESS_TOKEN")
        access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")

        try:
            self.auth = OAuthHandler(consumer_key, consumer_secret)
            self.auth.set_access_token(access_token, access_token_secret)
            self.api = tweepy.API(self.auth)
        except Exception as e:
            print("Error: Authentication Failed", str(e))
            raise

    @staticmethod
    def clean_tweet(tweet):
        """
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        """
        return " ".join(
            re.sub(
                r"(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet
            ).split()
        )

    def get_tweets(self, query, count=10, geo=None, lang="en"):
        """
        Main function to fetch tweets and parse them.
        """
        tweets = []
        tweets_ids = set()

        try:
            # call twitter api to fetch tweets
            fetched_tweets = self.api.search_tweets(
                q=query, count=count, geocode=geo, lang=lang
            )
            for tweet in fetched_tweets:
                parsed_tweet = {
                    "id": tweet.id_str,
                    "created_at": str(tweet.created_at),
                    "text": self.clean_tweet(tweet.text),
                }
                tweets_ids.add(tweet.id_str)

                if not parsed_tweet["text"]:
                    continue

                if tweet.retweet_count > 0:
                    # if tweet has retweets, ensure that it is appended only once
                    if parsed_tweet["id"] not in tweets_ids:
                        tweets.append(parsed_tweet)
                else:
                    tweets.append(parsed_tweet)

            return tweets

        except tweepy.TweepyException as e:
            print("Error : " + str(e))


def main():
    api = TwitterClient()
    tweets = api.get_tweets(query="weather", count=200, geo="41.3850639,2.1734035,5km")
    # 2.103621,41.343999,2.232702,41.449146  # https://boundingbox.klokantech.com/
    # center of Barcelona plsus 5km radius  41.3850639, 2.1734035, 5km
    print(tweets)


if __name__ == "__main__":
    main()
