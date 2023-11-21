import logging
from enum import Enum

import uvicorn
from fastapi import FastAPI, Query
import uuid
from datetime import datetime
from typing import Optional, List
import requests
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from pydantic import UUID4, BaseModel
from sqlmodel import Field, SQLModel
import pandas as pd
import json

app = FastAPI()

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
                "datalogger": 1,  # We assume this data comes from the same sensor, so we set its datalogger ID to 1
                "timestamp": timestamp,
                "precipitation": values["precip"],
                "temperature": values["temp"],
                "humidity": values["hum"]
            }
            formatted_data.append(formatted_entry)

    # Creating a DataFrame with 'id' as the index
    df = pd.DataFrame(formatted_data)
    df.set_index('id', inplace=True)

    try:
        # Read the existing CSV file
        existing_df = pd.read_csv("database.csv", index_col='id', parse_dates=['timestamp'])

        # Update or append rows based on the timestamp
        for idx, row in df.iterrows():
            if idx in existing_df.index:
                # Update existing row
                existing_df.loc[idx] = row
            else:
                # Append new row
                existing_df = existing_df.append(row)

        # Save the updated DataFrame to the CSV file
        existing_df.to_csv("database.csv")

        logging.info("Database updated successfully.")
    except FileNotFoundError:
        # If the file doesn't exist, save the current DataFrame to the CSV file
        df.to_csv("database.csv")
        logging.info("Database file not found. Created a new file.")
else:
    logging.error(f"Error: {request.status_code}")


##########Schemas##########

class MeasurementQueryParams(BaseModel):
    since: datetime = Query(None, description="Filter by date and time. Ingestion date of returned records should be higher than the value provided. Format expected ISO-8601.")
    before: datetime = Query(datetime.utcnow(), description="Filter by date and time. Ingestion date of returned records should be lower than the value provided. Default is now. Format expected ISO-8601.", allow_empty=True)
    span: str = Query(None, description="Aggregates data given this parameter. Default value should be raw (meaning no aggregate).", enum=["day", "hour", "max"])
    datalogger: str = Query(..., description="Filter by datalogger. This field is required. Should be an exact match of the datalogger id")

class LabelField(Enum):
    PRECIPITATION = "precip"
    TEMPERATURE = "temp"
    HUMIDITY = "hum"


class DataRecordResponse(BaseModel):
    label: LabelField
    measured_at: datetime
    value: float


class DataRecordAggregateResponse(BaseModel):
    label: LabelField
    time_slot: datetime
    value: float


##########Business part#########

def timescale_is_wrong(since, before):
    return since < before

# Function to retrieve data from the CSV file
def get_data_from_csv(query_params: MeasurementQueryParams):
    try:
        df = pd.read_csv("database.csv", index_col='id', parse_dates=['timestamp'])
        filtered_df = df[(df['timestamp'] >= query_params.since) & (df['timestamp'] <= query_params.before) & (df['datalogger'] == query_params.datalogger)]

        if query_params.span:
            # Aggregate data based on the specified span
            if query_params.span == "day":
                aggregated_df = filtered_df.resample('D').mean()
            elif query_params.span == "hour":
                aggregated_df = filtered_df.resample('H').mean()
            elif query_params.span == "max":
                aggregated_df = filtered_df.resample('D').max()

            # Reset index to make time_slot a separate column
            aggregated_df.reset_index(inplace=True)

            # Convert the DataFrame to a list of DataRecordAggregateResponse objects
            result = [
                DataRecordAggregateResponse(
                    label=LabelField(label),
                    time_slot=row['timestamp'],
                    value=row[metric]
                )
                for label, metric in [('temp', 'temp'), ('hum', 'hum'), ('precip', 'precip')]
                for _, row in aggregated_df.iterrows()
            ]
        else:
            # Convert the DataFrame to a list of DataRecordResponse objects
            result = [
                DataRecordResponse(
                    label=LabelField(row['label']),
                    measured_at=row['timestamp'],
                    value=row['value']
                )
                for _, row in filtered_df.iterrows()
            ]

        return result
    except FileNotFoundError:
        return []  # Return an empty list if the file is not found



##########API part#########

# Load the CSV file into a DataFrame
try:
    df = pd.read_csv("database.csv", index_col='id', parse_dates=['timestamp'])
except FileNotFoundError:
    df = pd.DataFrame(columns=['timestamp', 'precip', 'temp', 'hum'])


@app.get("/api/summary", response_model=List[DataRecordAggregateResponse])
async def api_fetch_data_aggregates(query_params: MeasurementQueryParams):
    return get_data_from_csv(query_params)


@app.get("/api/data", response_model=List[DataRecordResponse])
async def api_fetch_data_raw(query_params: MeasurementQueryParams):
    return get_data_from_csv(query_params)
