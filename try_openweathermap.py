import os
from pyowm import OWM
from pyowm.utils import config
from pyowm.utils import timestamps

# code taken from https://github.com/csparpa/pyowm
owm = OWM(os.getenv("api_key"))
mgr = owm.weather_manager()


# Search for current weather in Barcelona and get details
observation = mgr.weather_at_place('Barcelona,ES')
w = observation.weather

current_weather = [
w.detailed_status,         # 'clouds'
w.wind(),                  # {'speed': 4.6, 'deg': 330}
w.humidity,                # 87
w.temperature('celsius'),  # {'temp_max': 10.5, 'temp': 9.7, 'temp_min': 9.0}
w.rain,                    # {}
w.heat_index,              # None
w.clouds,                  # 75
]

print("\n".join([*(str(i) for i in current_weather)]))


