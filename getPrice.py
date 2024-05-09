import json
import requests
import pandas as pd
import datetime
import threading
import time
import os
from dotenv import load_dotenv

# Function to schedule and execute the task
def schedule_task(urlPrice: str, urlWeather: str):
    while True:
        df = getPriceFrame(urlPrice)
        df2 = getWeatherFrame(urlWeather)
         # Inner join on 'datetime' column
        result = pd.merge(df, df2, on='date_time', how='inner')
        print(result)
        time.sleep(5)  # Sleep for 5 seconds before next execution

def main():
    load_dotenv()

    # Get URLs from environment variables
    urlPrice = os.getenv("URL_PRICE")
    urlWeather = os.getenv("URL_WEATHER")

    # Create a thread for the scheduled task
    task_thread = threading.Thread(target=schedule_task(urlPrice, urlWeather))

    # Start the thread
    task_thread.start()

# Function to get weather data and return as DataFrame
def getWeatherFrame(url) -> pd.DataFrame:
    try:
        # Send GET request to weather API
        res = requests.get(url)

        if res.status_code != 200 :
            print("request not successfull with code: ", res.status_code)

        out = handle_weather_response(res.text)

    except Exception as err:
        df = pd.read_json(err)
        print("an error occured: ", err)
    
    finally:
        return out

# Function to get price data and return as DataFrame
def getPriceFrame(url) -> pd.DataFrame:
    try:
        res = requests.get(url)

        if res.status_code != 200 :
            print("request not successfull with code: ", res.status_code)

        out = handle_price_response(res.text)

    except Exception as err:
        df = pd.read_json(err)
        print("an error occured: ", err)
    
    finally:
        return out

# Funktion zur Umrechnung von Millisekunden seit 1970 in Datetime-Objekte
def milliseconds_to_datetime(ms):
    
    timestamp_sec = ms / 1000
    # Convert the timestamp to a datetime object
    dt_object = datetime.datetime.fromtimestamp(timestamp_sec)
    return dt_object

# Function to convert seconds since 1970 to datetime objects
def seconds_to_datetime(ms):
    # Convert the timestamp to a datetime object
    return datetime.datetime.fromtimestamp(ms)


# Function to handle price API response and return DataFrame
def handle_price_response(res: str) -> pd.DataFrame:

    data = json.loads(res)["data"]
    df = pd.DataFrame(data)

    if len(data) == 0:
        return "no data today, sorry"

    # change to datetime    
    df['date_time'] = df['start_timestamp'].apply(milliseconds_to_datetime)

    return df[['date_time', 'marketprice', 'unit']]
    
# Function to handle weather API response and return DataFrame
def handle_weather_response(res: str) -> pd.DataFrame:

    data = json.loads(res)["hourly"]["data"]

    df = pd.DataFrame(data)

    if len(data) == 0:
        return "no data today, sorry"
    
    # change to datetime    
    df['date_time'] = df['time'].apply(seconds_to_datetime)
    
    # during the day the sun_index depends on the cloudCover
    df['sun_index'] = 1 - df['cloudCover']

    # at night there is no sun 
    df.loc[df['icon'] == "clear-night", 'sun_index'] = 0

    return df[['date_time', 'sun_index']]


if __name__ == "__main__":
    main()