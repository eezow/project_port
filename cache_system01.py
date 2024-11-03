import redis
import requests
from apscheduler.schedulers.background import BackgroundScheduler
import time
import json

#Initialize Redis Connection
try:
    cache = redis.Redis(host='localhost' , port=6379, db=0, decode_responses=True)
    cache.ping()
    redis_available = True
    print("Connected to Redis Succesfully.")
except (redis.ConnectionError, redis.TimeoutError):
    redis_available = False
    print("Redis connection failed. Using in-memory cache")

#In-memory fallback cache
in_memory_cache = {}


# API and cache keys
CITIES = ["Johannesburg", "Cape Town", "Durban", "Pretoria", "Port Elizabeth"]
#WEATHER_API = "https://api.openweathermap.org/data/2.5/weather?q=London&appid=c45640f19f871edb471d843c64487f20"
CACHE_KEY = "weather_data"

# Initialize cache performance counters
CACHE_HIT_KEY = "cache_hit_count"
CACHE_MISS_KEY = "cache_miss_count"

# Initialize cache performance counters
if redis_available:
    cache.set(CACHE_HIT_KEY, 0)
    cache.set(CACHE_MISS_KEY, 0)
else:
    in_memory_cache[CACHE_HIT_KEY] = 0
    in_memory_cache[CACHE_MISS_KEY] = 0


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
        if redis_available:
            cache.set(city, json.dumps(simplified_data))
            cache.expire(city, 600)
        else:
            in_memory_cache[city] = {"data": simplified_data, "expiry": time.time() + 600}

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
    if redis_available:
        data = cache.get(city)
        if data is None:
            print(f"Cache miss for {city} - fetching new data")
            fetch_weather_data(city)
            data = cache.get(city)
            cache.incr(CACHE_MISS_KEY)
        else:
            print(f"Cache hit for {city}")
            cache.incr(CACHE_HIT_KEY) #increment hit counter
    else:
        cached = in_memory_cache.get(city)
        if cached and time.time() < cached["expiry"]:
            print(f"Cache hit in in-memory cache for {city}")
            in_memory_cache[CACHE_HIT_KEY] += 1
            data = json.dumps(cached["data"]) #Convert to JSON
        else:
            print(f"Cache miss in in-memory cache for {city} - fetching new data")
            fetch_weather_data(city)
            cached = in_memory_cache.get(city)
            in_memory_cache[CACHE_MISS_KEY] += 1
            data = json.dumps(cached["data"]) if cached else None

    if data:
        # format new data
        data = json.loads(data)
        return f"Weather in {data['city']}: {data['temperature']} degrees C, {data['description']}"
    return "Weather data unavailable."

def display_cache_perfomance():
    if redis_available:

        cache_hits = cache.get(CACHE_HIT_KEY)
        cache_misses = cache.get(CACHE_MISS_KEY)
    else:
        cache_hits = in_memory_cache.get(CACHE_HIT_KEY, 0)
        cache_misses = in_memory_cache.get(CACHE_MISS_KEY, 0)

    print(f"\nCache Performance: Hits = {cache_hits}, Misses = {cache_misses}\n")

#simulate application running and displaying data for each city
try:
    while True:
        for city in CITIES:
             print(get_weather_data(city))
        time.sleep(60)
        display_cache_perfomance()
except (keyboardInterrupt, SystemExit):
    scheduler.shutdown()
