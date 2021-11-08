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
            print("Error: Twitter Authentication Failed", str(e))
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

    def get_tweets(self, query="", count=10, geo=None, lang="en", until="2021-01-01", tweet_parser=None):
        """
        Main function to fetch tweets.
        Pass a tweet_parser function to parse the tweet otherwise raw json format is returned
        """
        tweets = []
        tweets_ids = set()

        def parse_tweet(tweet, tweet_parser=None):
            if tweet_parser:
                return tweet_parser(tweet)
            else:
                return tweet

        try:
            fetched_tweets = self.api.search_tweets(
                q=query, count=count, geocode=geo, lang=lang, until=until
            )
        except tweepy.TweepyException as e:
            print("Error : " + str(e))

        for tweet in fetched_tweets:
            tweets_ids.add(tweet.id_str)
            if tweet.retweet_count > 0:
                # if tweet has retweets, ensure that it is appended only once
                if tweet.id_str not in tweets_ids:
                    tweets.append(parse_tweet(tweet=tweet, tweet_parser=tweet_parser))
            else:
                tweets.append(parse_tweet(tweet=tweet, tweet_parser=tweet_parser))

        return tweets


def main():
    api = TwitterClient()
    tweets = api.get_tweets(query="", count=200, geo="41.3850639,2.1734035,5km")
    # 2.103621,41.343999,2.232702,41.449146  # https://boundingbox.klokantech.com/
    # center of Barcelona plus 5km radius  41.3850639, 2.1734035, 5km
    print(tweets)
    print(f"Fetched {len(tweets)} tweets.")


if __name__ == "__main__":
    main()


# Sample raw tweet
"""
tweet
Status(_api=<tweepy.api.API object at 0x10547f0d0>, _json={'created_at': 'Fri Nov 05 20:39:23 +0000 2021', 'id': 1456722835825627136, 'id_str': '1456722835825627136', 'text': '@ehreninvestor_ 😂😂😂 sorry', 'truncated': False, 'entities': {'hashtags': [], 'symbols': [], 'user_mentions': [{'screen_name': 'ehreninvestor_', 'name': 'ehreninvestor_', 'id': 4699607953, 'id_str': '4699607953', 'indices': [0, 15]}], 'urls': []}, 'metadata': {'iso_language_code': 'en', 'result_type': 'recent'}, 'source': '<a href="http://twitter.com/download/iphone" rel="nofollow">Twitter for iPhone</a>', 'in_reply_to_status_id': 1456722219879514116, 'in_reply_to_status_id_str': '1456722219879514116', 'in_reply_to_user_id': 4699607953, 'in_reply_to_user_id_str': '4699607953', 'in_reply_to_screen_name': 'ehreninvestor_', 'user': {'id': 1336224903641632768, 'id_str': '1336224903641632768', 'name': 'Logic', 'screen_name': 'BlaugranaLogic', 'location': 'Camp Nou', 'description': '💙❤️ & 💚🤍🖤 Barça fan and Fohlen follower. ☕Coffee addict', 'url': None, 'entities': {'description': {'urls': []}}, 'protected': False, 'followers_count': 408, 'friends_count': 409, 'listed_count': 0, 'created_at': 'Tue Dec 08 08:23:46 +0000 2020', 'favourites_count': 9011, 'utc_offset': None, 'time_zone': None, 'geo_enabled': False, 'verified': False, 'statuses_count': 6300, 'lang': None, 'contributors_enabled': False, 'is_translator': False, 'is_translation_enabled': False, 'profile_background_color': 'F5F8FA', 'profile_background_image_url': None, 'profile_background_image_url_https': None, 'profile_background_tile': False, 'profile_image_url': 'http://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', 'profile_image_url_https': 'https://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', 'profile_banner_url': 'https://pbs.twimg.com/profile_banners/1336224903641632768/1625972588', 'profile_link_color': '1DA1F2', 'profile_sidebar_border_color': 'C0DEED', 'profile_sidebar_fill_color': 'DDEEF6', 'profile_text_color': '333333', 'profile_use_background_image': True, 'has_extended_profile': True, 'default_profile': True, 'default_profile_image': False, 'following': False, 'follow_request_sent': False, 'notifications': False, 'translator_type': 'none', 'withheld_in_countries': []}, 'geo': None, 'coordinates': None, 'place': None, 'contributors': None, 'is_quote_status': False, 'retweet_count': 0, 'favorite_count': 0, 'favorited': False, 'retweeted': False, 'lang': 'en'}, created_at=datetime.datetime(2021, 11, 5, 20, 39, 23, tzinfo=datetime.timezone.utc), id=1456722835825627136, id_str='1456722835825627136', text='@ehreninvestor_ 😂😂😂 sorry', truncated=False, entities={'hashtags': [], 'symbols': [], 'user_mentions': [{'screen_name': 'ehreninvestor_', 'name': 'ehreninvestor_', 'id': 4699607953, 'id_str': '4699607953', 'indices': [0, 15]}], 'urls': []}, metadata={'iso_language_code': 'en', 'result_type': 'recent'}, source='Twitter for iPhone', source_url='http://twitter.com/download/iphone', in_reply_to_status_id=1456722219879514116, in_reply_to_status_id_str='1456722219879514116', in_reply_to_user_id=4699607953, in_reply_to_user_id_str='4699607953', in_reply_to_screen_name='ehreninvestor_', author=User(_api=<tweepy.api.API object at 0x10547f0d0>, _json={'id': 1336224903641632768, 'id_str': '1336224903641632768', 'name': 'Logic', 'screen_name': 'BlaugranaLogic', 'location': 'Camp Nou', 'description': '💙❤️ & 💚🤍🖤 Barça fan and Fohlen follower. ☕Coffee addict', 'url': None, 'entities': {'description': {'urls': []}}, 'protected': False, 'followers_count': 408, 'friends_count': 409, 'listed_count': 0, 'created_at': 'Tue Dec 08 08:23:46 +0000 2020', 'favourites_count': 9011, 'utc_offset': None, 'time_zone': None, 'geo_enabled': False, 'verified': False, 'statuses_count': 6300, 'lang': None, 'contributors_enabled': False, 'is_translator': False, 'is_translation_enabled': False, 'profile_background_color': 'F5F8FA', 'profile_background_image_url': None, 'profile_background_image_url_https': None, 'profile_background_tile': False, 'profile_image_url': 'http://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', 'profile_image_url_https': 'https://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', 'profile_banner_url': 'https://pbs.twimg.com/profile_banners/1336224903641632768/1625972588', 'profile_link_color': '1DA1F2', 'profile_sidebar_border_color': 'C0DEED', 'profile_sidebar_fill_color': 'DDEEF6', 'profile_text_color': '333333', 'profile_use_background_image': True, 'has_extended_profile': True, 'default_profile': True, 'default_profile_image': False, 'following': False, 'follow_request_sent': False, 'notifications': False, 'translator_type': 'none', 'withheld_in_countries': []}, id=1336224903641632768, id_str='1336224903641632768', name='Logic', screen_name='BlaugranaLogic', location='Camp Nou', description='💙❤️ & 💚🤍🖤 Barça fan and Fohlen follower. ☕Coffee addict', url=None, entities={'description': {'urls': []}}, protected=False, followers_count=408, friends_count=409, listed_count=0, created_at=datetime.datetime(2020, 12, 8, 8, 23, 46, tzinfo=datetime.timezone.utc), favourites_count=9011, utc_offset=None, time_zone=None, geo_enabled=False, verified=False, statuses_count=6300, lang=None, contributors_enabled=False, is_translator=False, is_translation_enabled=False, profile_background_color='F5F8FA', profile_background_image_url=None, profile_background_image_url_https=None, profile_background_tile=False, profile_image_url='http://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', profile_image_url_https='https://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', profile_banner_url='https://pbs.twimg.com/profile_banners/1336224903641632768/1625972588', profile_link_color='1DA1F2', profile_sidebar_border_color='C0DEED', profile_sidebar_fill_color='DDEEF6', profile_text_color='333333', profile_use_background_image=True, has_extended_profile=True, default_profile=True, default_profile_image=False, following=False, follow_request_sent=False, notifications=False, translator_type='none', withheld_in_countries=[]), user=User(_api=<tweepy.api.API object at 0x10547f0d0>, _json={'id': 1336224903641632768, 'id_str': '1336224903641632768', 'name': 'Logic', 'screen_name': 'BlaugranaLogic', 'location': 'Camp Nou', 'description': '💙❤️ & 💚🤍🖤 Barça fan and Fohlen follower. ☕Coffee addict', 'url': None, 'entities': {'description': {'urls': []}}, 'protected': False, 'followers_count': 408, 'friends_count': 409, 'listed_count': 0, 'created_at': 'Tue Dec 08 08:23:46 +0000 2020', 'favourites_count': 9011, 'utc_offset': None, 'time_zone': None, 'geo_enabled': False, 'verified': False, 'statuses_count': 6300, 'lang': None, 'contributors_enabled': False, 'is_translator': False, 'is_translation_enabled': False, 'profile_background_color': 'F5F8FA', 'profile_background_image_url': None, 'profile_background_image_url_https': None, 'profile_background_tile': False, 'profile_image_url': 'http://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', 'profile_image_url_https': 'https://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', 'profile_banner_url': 'https://pbs.twimg.com/profile_banners/1336224903641632768/1625972588', 'profile_link_color': '1DA1F2', 'profile_sidebar_border_color': 'C0DEED', 'profile_sidebar_fill_color': 'DDEEF6', 'profile_text_color': '333333', 'profile_use_background_image': True, 'has_extended_profile': True, 'default_profile': True, 'default_profile_image': False, 'following': False, 'follow_request_sent': False, 'notifications': False, 'translator_type': 'none', 'withheld_in_countries': []}, id=1336224903641632768, id_str='1336224903641632768', name='Logic', screen_name='BlaugranaLogic', location='Camp Nou', description='💙❤️ & 💚🤍🖤 Barça fan and Fohlen follower. ☕Coffee addict', url=None, entities={'description': {'urls': []}}, protected=False, followers_count=408, friends_count=409, listed_count=0, created_at=datetime.datetime(2020, 12, 8, 8, 23, 46, tzinfo=datetime.timezone.utc), favourites_count=9011, utc_offset=None, time_zone=None, geo_enabled=False, verified=False, statuses_count=6300, lang=None, contributors_enabled=False, is_translator=False, is_translation_enabled=False, profile_background_color='F5F8FA', profile_background_image_url=None, profile_background_image_url_https=None, profile_background_tile=False, profile_image_url='http://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', profile_image_url_https='https://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', profile_banner_url='https://pbs.twimg.com/profile_banners/1336224903641632768/1625972588', profile_link_color='1DA1F2', profile_sidebar_border_color='C0DEED', profile_sidebar_fill_color='DDEEF6', profile_text_color='333333', profile_use_background_image=True, has_extended_profile=True, default_profile=True, default_profile_image=False, following=False, follow_request_sent=False, notifications=False, translator_type='none', withheld_in_countries=[]), geo=None, coordinates=None, place=None, contributors=None, is_quote_status=False, retweet_count=0, favorite_count=0, favorited=False, retweeted=False, lang='en')
special variables:
function variables:
author: User(_api=<tweepy.api.API object at 0x10547f0d0>, _json={'id': 1336224903641632768, 'id_str': '1336224903641632768', 'name': 'Logic', 'screen_name': 'BlaugranaLogic', 'location': 'Camp Nou', 'description': '💙❤️ & 💚🤍🖤 Barça fan and Fohlen follower. ☕Coffee addict', 'url': None, 'entities': {'description': {'urls': []}}, 'protected': False, 'followers_count': 408, 'friends_count': 409, 'listed_count': 0, 'created_at': 'Tue Dec 08 08:23:46 +0000 2020', 'favourites_count': 9011, 'utc_offset': None, 'time_zone': None, 'geo_enabled': False, 'verified': False, 'statuses_count': 6300, 'lang': None, 'contributors_enabled': False, 'is_translator': False, 'is_translation_enabled': False, 'profile_background_color': 'F5F8FA', 'profile_background_image_url': None, 'profile_background_image_url_https': None, 'profile_background_tile': False, 'profile_image_url': 'http://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', 'profile_image_url_https': 'https://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', 'profile_banner_url': 'https://pbs.twimg.com/profile_banners/1336224903641632768/1625972588', 'profile_link_color': '1DA1F2', 'profile_sidebar_border_color': 'C0DEED', 'profile_sidebar_fill_color': 'DDEEF6', 'profile_text_color': '333333', 'profile_use_background_image': True, 'has_extended_profile': True, 'default_profile': True, 'default_profile_image': False, 'following': False, 'follow_request_sent': False, 'notifications': False, 'translator_type': 'none', 'withheld_in_countries': []}, id=1336224903641632768, id_str='1336224903641632768', name='Logic', screen_name='BlaugranaLogic', location='Camp Nou', description='💙❤️ & 💚🤍🖤 Barça fan and Fohlen follower. ☕Coffee addict', url=None, entities={'description': {'urls': []}}, protected=False, followers_count=408, friends_count=409, listed_count=0, created_at=datetime.datetime(2020, 12, 8, 8, 23, 46, tzinfo=datetime.timezone.utc), favourites_count=9011, utc_offset=None, time_zone=None, geo_enabled=False, verified=False, statuses_count=6300, lang=None, contributors_enabled=False, is_translator=False, is_translation_enabled=False, profile_background_color='F5F8FA', profile_background_image_url=None, profile_background_image_url_https=None, profile_background_tile=False, profile_image_url='http://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', profile_image_url_https='https://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', profile_banner_url='https://pbs.twimg.com/profile_banners/1336224903641632768/1625972588', profile_link_color='1DA1F2', profile_sidebar_border_color='C0DEED', profile_sidebar_fill_color='DDEEF6', profile_text_color='333333', profile_use_background_image=True, has_extended_profile=True, default_profile=True, default_profile_image=False, following=False, follow_request_sent=False, notifications=False, translator_type='none', withheld_in_countries=[])
contributors: None
coordinates: None
created_at: datetime.datetime(2021, 11, 5, 20, 39, 23, tzinfo=datetime.timezone.utc)
entities: {'hashtags': [], 'symbols': [], 'user_mentions': [{...}], 'urls': []}
favorite_count: 0
favorited: False
geo: None
id: 1456722835825627136
id_str: '1456722835825627136'
in_reply_to_screen_name: 'ehreninvestor_'
in_reply_to_status_id: 1456722219879514116
in_reply_to_status_id_str: '1456722219879514116'
in_reply_to_user_id: 4699607953
in_reply_to_user_id_str: '4699607953'
is_quote_status: False
lang: 'en'
metadata: {'iso_language_code': 'en', 'result_type': 'recent'}
place: None
retweet_count: 0
retweeted: False
source: 'Twitter for iPhone'
source_url: 'http://twitter.com/download/iphone'
text: '@ehreninvestor_ 😂😂😂 sorry'
truncated: False
user: User(_api=<tweepy.api.API object at 0x10547f0d0>, _json={'id': 1336224903641632768, 'id_str': '1336224903641632768', 'name': 'Logic', 'screen_name': 'BlaugranaLogic', 'location': 'Camp Nou', 'description': '💙❤️ & 💚🤍🖤 Barça fan and Fohlen follower. ☕Coffee addict', 'url': None, 'entities': {'description': {'urls': []}}, 'protected': False, 'followers_count': 408, 'friends_count': 409, 'listed_count': 0, 'created_at': 'Tue Dec 08 08:23:46 +0000 2020', 'favourites_count': 9011, 'utc_offset': None, 'time_zone': None, 'geo_enabled': False, 'verified': False, 'statuses_count': 6300, 'lang': None, 'contributors_enabled': False, 'is_translator': False, 'is_translation_enabled': False, 'profile_background_color': 'F5F8FA', 'profile_background_image_url': None, 'profile_background_image_url_https': None, 'profile_background_tile': False, 'profile_image_url': 'http://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', 'profile_image_url_https': 'https://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', 'profile_banner_url': 'https://pbs.twimg.com/profile_banners/1336224903641632768/1625972588', 'profile_link_color': '1DA1F2', 'profile_sidebar_border_color': 'C0DEED', 'profile_sidebar_fill_color': 'DDEEF6', 'profile_text_color': '333333', 'profile_use_background_image': True, 'has_extended_profile': True, 'default_profile': True, 'default_profile_image': False, 'following': False, 'follow_request_sent': False, 'notifications': False, 'translator_type': 'none', 'withheld_in_countries': []}, id=1336224903641632768, id_str='1336224903641632768', name='Logic', screen_name='BlaugranaLogic', location='Camp Nou', description='💙❤️ & 💚🤍🖤 Barça fan and Fohlen follower. ☕Coffee addict', url=None, entities={'description': {'urls': []}}, protected=False, followers_count=408, friends_count=409, listed_count=0, created_at=datetime.datetime(2020, 12, 8, 8, 23, 46, tzinfo=datetime.timezone.utc), favourites_count=9011, utc_offset=None, time_zone=None, geo_enabled=False, verified=False, statuses_count=6300, lang=None, contributors_enabled=False, is_translator=False, is_translation_enabled=False, profile_background_color='F5F8FA', profile_background_image_url=None, profile_background_image_url_https=None, profile_background_tile=False, profile_image_url='http://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', profile_image_url_https='https://pbs.twimg.com/profile_images/1455656446130114565/kdfNk-Uf_normal.jpg', profile_banner_url='https://pbs.twimg.com/profile_banners/1336224903641632768/1625972588', profile_link_color='1DA1F2', profile_sidebar_border_color='C0DEED', profile_sidebar_fill_color='DDEEF6', profile_text_color='333333', profile_use_background_image=True, has_extended_profile=True, default_profile=True, default_profile_image=False, following=False, follow_request_sent=False, notifications=False, translator_type='none', withheld_in_countries=[])
_api: <tweepy.api.API object at 0x10547f0d0>
_json: {'created_at': 'Fri Nov 05 20:39:23 +0000 2021', 'id': 1456722835825627136, 'id_str': '1456722835825627136', 'text': '@ehreninvestor_ 😂😂😂 sorry', 'truncated': False, 'entities': {'hashtags': [...], 'symbols': [...], 'user_mentions': [...], 'urls': [...]}, 'metadata': {'iso_language_code': 'en', 'result_type': 'recent'}, 'source': '<a href="http://twit...iPhone</a>', 'in_reply_to_status_id': 1456722219879514116, 'in_reply_to_status_id_str': '1456722219879514116', 'in_reply_to_user_id': 4699607953, 'in_reply_to_user_id_str': '4699607953', 'in_reply_to_screen_name': 'ehreninvestor_', 'user': {'id': 1336224903641632768, 'id_str': '1336224903641632768', 'name': 'Logic', 'screen_name': 'BlaugranaLogic', 'location': 'Camp Nou', 'description': '💙❤️ & 💚🤍🖤 Barça fan ...fee addict', 'url': None, 'entities': {...}, 'protected': False, ...}, ...}
"""
