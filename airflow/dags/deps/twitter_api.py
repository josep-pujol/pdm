import os
import re
from typing import Any, Callable, List, Set, Union
import tweepy
from tweepy.models import Status
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
        consumer_key: str = os.getenv("CONSUMER_KEY")
        consumer_secret: str = os.getenv("CONSUMER_SECRET")
        access_token: str = os.getenv("ACCESS_TOKEN")
        access_token_secret: str = os.getenv("ACCESS_TOKEN_SECRET")

        try:
            self.auth = OAuthHandler(consumer_key, consumer_secret)
            self.auth.set_access_token(access_token, access_token_secret)
            self.api = tweepy.API(self.auth)
        except Exception as err:
            print("Error: Twitter Authentication Failed", str(err))

    @staticmethod
    def parse_tweet(tweet, tweet_parser: Callable = None) -> Any:
        """parses a tweet if parser function is passed
        otherwise just returns same object passed"""
        if tweet_parser:
            return tweet_parser(tweet)
        else:
            return tweet

    def fetch_tweets(
        self,
        query: str = "",
        count: int = 10,
        geo: Union[str, None] = None,
        lang: str = "en",
        until: Union[str, None] = None,
    ):
        """Calls Tweepy api to fetch tweets as per parameters passed"""
        try:
            return self.api.search_tweets(
                q=query, count=count, geocode=geo, lang=lang, until=until
            )
        except tweepy.TweepyException as err:
            print("Error : " + str(err))
            raise

    @staticmethod
    def remove_duplicate_tweets(tweets):
        """To remove duplicate tweets or retweets"""
        return {tweet for tweet in tweets}

    def get_tweets(
        self,
        query: str = "",
        count: int = 10,
        geo: Union[str, None] = None,
        lang: str = "en",
        until: Union[str, None] = None,
        tweet_parser: Callable = None,
    ) -> List[Any]:
        """
        Main function to fetch tweets.
        Pass a tweet_parser function to parse the tweet otherwise a list of
        tweepy objects is returned

        Args:
            until: must be date in string format YYYY-MM-DD in the last 7 days
        Returns:
            List of tweets
        """
        fetched_tweets = self.fetch_tweets(
            query=query, count=count, geo=geo, lang=lang, until=until
        )
        unique_tweets = TwitterClient.remove_duplicate_tweets(fetched_tweets)
        return [
            TwitterClient.parse_tweet(tweet=tweet, tweet_parser=tweet_parser)
            for tweet in unique_tweets
        ]


def main():
    """For testing purposes"""
    api = TwitterClient()
    # 2.103621,41.343999,2.232702,41.449146  # https://boundingbox.klokantech.com/
    # center of Barcelona plus 5km radius  41.3850639, 2.1734035, 5km
    tweets = api.get_tweets(
        query="", count=1000, geo="41.3850639,2.1734035,5km", until="2021-12-14"
    )
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
