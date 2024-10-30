import redis
import requests
from apscheduler.schedulers.background import BackgroundScheduler
import time
import json

#connect to redis
cache = redis.Redis(host='localhost' , port=6379, db=0, decode_responses=True)

# API and cache keys
CITIES = ["Johannesburg", "Cape Town", "Durban", "Pretoria", "Port Elizabeth"]
#WEATHER_API = "https://api.openweathermap.org/data/2.5/weather?q=London&appid=c45640f19f871edb471d843c64487f20"
CACHE_KEY = "weather_data"

def fetch_weather_data(city):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid=c45640f19f871edb471d843c64487f20"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
       

        #extract only the essentials
        simplified_data = {
             "city": data["name"],
             "temperature": round(data["main"]["temp"] - 273.15,2),
             "description": data["weather"][0]["description"].capitalize()

        }

        # store cleaned data in redis
        cache.set(city, json.dumps(simplified_data))
        cache.expire(city, 600)
        print(f"Data refreshed for {city}: {simplified_data}")
    else:
        print(f"Error Fetching data for {city}: {response.status_code}")



# Initialize the scheduler
scheduler = BackgroundScheduler()
for city in CITIES:
    scheduler.add_job(fetch_weather_data, 'interval', minutes=10, args=[city])
scheduler.start()

#initialize fetch to produce cache on startup
for city in CITIES:
    fetch_weather_data(city)

#Main function to retrieve weather data from cache
def get_weather_data(city):
    data = cache.get(CACHE_KEY)
    if data is None:
        print("Cache miss - fetching new data")
        fetch_weather_data(city)
        data = cache.get(city)
    else:
        print("Cache hit")
    
    # format new data
    data = json.loads(data)
    return f"Weather in {data['city']}: {data['temperature']} degrees C, {data['description']}"


try:
    while True:
        for city in CITIES:
             print(get_weather_data(city))
        time.sleep(60)
except (keyboardInterrupt, SystemExit):
    scheduler.shutdown()
