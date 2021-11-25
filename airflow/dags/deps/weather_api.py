import os
from typing import ValuesView
from pyowm import OWM

from dotenv import load_dotenv

load_dotenv()


try:
    api_key: str = os.getenv("API_KEY")
    owm = OWM(api_key)
    mgr = owm.weather_manager()
except Exception as err:
    if not (api_key):
        raise ValueError("Missing API Key")
    print("Error: Owm authentication failed: ", err)


def get_weather(owm_location: str = "Barcelona,ES"):
    """Search for current weather in a OpenWeatherMap defined location"""
    return mgr.weather_at_place(owm_location)


def main():
    """For testing purposes"""
    print(get_weather().to_dict())


if __name__ == "__main__":
    main()
