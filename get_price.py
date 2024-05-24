"""Module extracting, transforming and loading waether/energy price data."""

import datetime
import threading
import time
import os
import json
import requests
import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv


def main():
    """Function to handle application flow."""

    load_dotenv()

    # Get URLs from environment variables
    url_price = os.getenv("URL_PRICE")
    url_weather = os.getenv("URL_WEATHER")

    # Create a thread for the scheduled task
    task_thread = threading.Thread(target=schedule_task(url_price, url_weather))

    # Start the thread
    task_thread.start()


def schedule_task(url_price: str, url_weather: str):
    """Function to schedule and execute the task."""
    while True:
        extract_price_frame(url_price)
        extract_weather_frame(url_weather)
        # Inner join on 'datetime' column
        # result = pd.merge(df, df2, on="date_time", how="inner")
        print("task done")
        time.sleep(5)  # Sleep for 5 seconds before next execution


def extract_weather_frame(url):
    """Function to get weather data and write file to disk."""
    try:
        # Send GET request to weather API
        res = requests.get(url, timeout=10)

        if res.status_code != 200:
            print("request not successfull with code: ", res.status_code)

        out = handle_weather_response(res.text)

    except requests.exceptions.RequestException as err:
        df = pd.read_json(err)
        print("an error occured: ", err)


def make_stamped_filename(name: str) -> str:
    """Function to craate a filename with timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"{name}_response_{timestamp}.parquet"


def extract_price_frame(url):
    """Function to get price data and store it in a paraquet file."""
    try:
        res = requests.get(url, timeout=10)

        if res.status_code != 200:
            print("request not successfull with code: ", res.status_code)

        df = pd.DataFrame(res)

        pa.write_table(pa.Table.from_pandas(df), make_stamped_filename("price"))

    except requests.exceptions.RequestException as err:
        df = pd.read_json(err)
        print("an error occured: ", err)


def milliseconds_to_datetime(ms) -> datetime.datetime:
    """Funktion zur Umrechnung von Millisekunden seit 1970 in Datetime-Objekte"""
    timestamp_sec = ms / 1000
    # Convert the timestamp to a datetime object
    dt_object = datetime.datetime.fromtimestamp(timestamp_sec)
    return dt_object


def seconds_to_datetime(ms) -> datetime.datetime:
    """Convert the timestamp to a datetime object."""
    return datetime.datetime.fromtimestamp(ms)


def handle_price_response(res: str) -> pd.DataFrame:
    """Function to handle price API response and return DataFrame."""

    data = json.loads(res)["data"]
    df = pd.DataFrame(data)

    if len(data) == 0:
        return "no data today, sorry"

    # change to datetime
    df["date_time"] = df["start_timestamp"].apply(milliseconds_to_datetime)

    return df[["date_time", "marketprice", "unit"]]


def handle_weather_response(res: str) -> pd.DataFrame:
    """Function to handle weather API response and return DataFrame."""

    data = json.loads(res)["hourly"]["data"]

    df = pd.DataFrame(data)

    if len(data) == 0:
        return "no data today, sorry"

    # change to datetime
    df["date_time"] = df["time"].apply(seconds_to_datetime)

    # during the day the sun_index depends on the cloudCover
    df["sun_index"] = 1 - df["cloudCover"]

    # at night there is no sun
    df.loc[df["icon"] == "clear-night", "sun_index"] = 0

    return df[["date_time", "sun_index"]]


if __name__ == "__main__":
    main()
