import logging
import uuid
from datetime import datetime
from enum import Enum
from typing import List, Optional

import json

import fastapi
import numpy as np
import pandas as pd
import requests
from fastapi import FastAPI, Query
from pydantic import BaseModel
from starlette.responses import FileResponse

app = FastAPI(default_response_class=fastapi.responses.ORJSONResponse)


@app.get("/")
async def read_index():
    return FileResponse('app/resources/static/index.html')


##########ETL part#########
url = "http://localhost:3000/measurements"
request = requests.get(url)
if request.status_code == 200:
    data = request.json()

    # Extracting data from the JSON and converting timestamp to datetime
    formatted_data = []
    for entry in data:
        for timestamp, values in entry.items():
            formatted_entry = {
                "id": str(uuid.uuid4()),  # Generate a unique ID for each row
                "datalogger": "1",  # We assume this data comes from the same sensor, so we set its datalogger ID to 1
                "date": timestamp,
                "precip": values["precip"],
                "temp": values["temp"],
                "hum": values["hum"]
            }
            formatted_data.append(formatted_entry)

    # Creating a DataFrame with 'id' as the index
    df = pd.DataFrame(formatted_data)
    df['date'] = pd.to_datetime(df['date'], unit='ms')
    df = df.replace([np.inf, -np.inf, np.nan], None)
    df.set_index('id', inplace=True)

    df.to_csv("database.csv")
    logging.info(
        "Database created successfully.")  # ici on se permet de créer la base de données purement et simplement.
    # On pourrait ajouter une update de la base de données à chaque fois que le dataframe contient des données plus
    # récentes que la plus récente donnée de la bdd

else:
    logging.error(f"Error: {request.status_code}")


##########Schemas##########


class LabelField(Enum):
    PRECIPITATION = "precip"
    TEMPERATURE = "temp"
    HUMIDITY = "hum"


class DataRecordResponse(BaseModel):
    label: LabelField
    measured_at: datetime
    value: Optional[float]


class DataRecordAggregateResponse(BaseModel):
    label: str
    time_slot: datetime
    value: Optional[float]


##########Business part#########

def timescale_is_wrong(since, before):
    return since < before


# Function to retrieve data from the CSV file
def get_data_from_csv(datalogger: str, before: datetime, since: datetime, span: str):
    # Filter data based on datalogger
    filtered_df = df[df['datalogger'] == int(datalogger)]

    # Set default value for since if it is None
    if since is None:
        since = filtered_df['date'].min()

    # Filter data based on date range
    filtered_df = filtered_df[(filtered_df['date'] >= since) & (filtered_df['date'] <= before)]

    if span == "raw":
        return map_raw_data(filtered_df)
    else:
        return map_aggregate_data(filtered_df, span)


# Helper function to map raw data
def map_raw_data(filtered_df):
    records = []
    for index, row in filtered_df.iterrows():
        records.append(DataRecordResponse(
            label=LabelField.PRECIPITATION,
            measured_at=row['date'],
            value=row['precip'] if not pd.isna(row['precip']) else None
        ))
        records.append(DataRecordResponse(
            label=LabelField.TEMPERATURE,
            measured_at=row['date'],
            value=row['temp'] if not pd.isna(row['precip']) else None
        ))
        records.append(DataRecordResponse(
            label=LabelField.HUMIDITY,
            measured_at=row['date'],
            value=row['hum'] if not pd.isna(row['precip']) else None
        ))
    return records


# Helper function to map aggregate data
def map_aggregate_data(filtered_df, span):
    # Implement aggregation logic based on the span (e.g., hourly, daily)
    aggregated_df = filtered_df.resample(span, on='date').mean().dropna()

    records = []
    for index, row in aggregated_df.iterrows():
        records.append(DataRecordAggregateResponse(
            label=LabelField.PRECIPITATION.value,
            time_slot=index,
            value=row['precip'] if not pd.isna(row['precip']) else None
        ))
        records.append(DataRecordAggregateResponse(
            label=LabelField.TEMPERATURE.value,
            time_slot=index,
            value=row['temp'] if not pd.isna(row['precip']) else None
        ))
        records.append(DataRecordAggregateResponse(
            label=LabelField.HUMIDITY.value,
            time_slot=index,
            value=row['hum'] if not pd.isna(row['precip']) else None
        ))
    return records


##########API part#########

# Load the CSV file into a DataFrame
try:
    df = pd.read_csv("database.csv", index_col='id', parse_dates=['date'])
except FileNotFoundError:
    df = pd.DataFrame(columns=['date', 'precip', 'temp', 'hum'])


@app.get("/api/summary", response_model=List[DataRecordAggregateResponse])
async def api_fetch_data_aggregates(datalogger: str, before: datetime = Query(datetime.now()),
                                    since: datetime = Query(datetime.now()), span: str | None = "raw"):
    return get_data_from_csv(datalogger, before, since, span)


@app.get("/api/data", response_model=List[DataRecordResponse])
async def api_fetch_data_raw(datalogger: str, before: datetime = Query(datetime.now()),
                             since: datetime = Query(None)):
    return get_data_from_csv(datalogger, before, since, "raw")
