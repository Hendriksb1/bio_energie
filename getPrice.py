import json
import requests
import pandas as pd
import datetime
import os
from dotenv import load_dotenv


# url = "https://api.corrently.io/v2.0/gsi/marketdata?zip=59077"


def main():

    load_dotenv()
    
    urlPrice = os.getenv("URL_PRICE")
    urlWeather = os.getenv("URL_WEATHER")

    df = getPriceFrame(urlPrice)
    df2 = getWeatherFrame(urlWeather)

    # Inner join on 'datetime' column
    result = pd.merge(df, df2, on='date_time', how='inner')
    print(result)


def getWeatherFrame(url) -> pd.DataFrame:
    try:
        res = requests.get(url)

        if res.status_code != 200 :
            print("request not successfull with code: ", res.status_code)

        out = handle_weather_response(res.text)

    except Exception as err:
        df = pd.read_json(err)
        print("an error occured: ", err)
    
    finally:
        return out

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

def seconds_to_datetime(ms):
    # Convert the timestamp to a datetime object
    return datetime.datetime.fromtimestamp(ms)

def handle_price_response(res: str) -> pd.DataFrame:

    data = json.loads(res)["data"]
    df = pd.DataFrame(data)

    if len(data) == 0:
        return "no data today, sorry"

    # change to datetime    
    df['date_time'] = df['start_timestamp'].apply(milliseconds_to_datetime)

    return df[['date_time', 'marketprice', 'unit']]
    

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

    print(df)

    return df[['date_time', 'sun_index']]


if __name__ == "__main__":
    main()