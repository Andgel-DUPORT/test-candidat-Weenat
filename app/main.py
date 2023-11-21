import logging
import uuid
from datetime import datetime
from enum import Enum
from typing import List

import pandas as pd
import requests
from fastapi import FastAPI, Query
from pydantic import BaseModel
from starlette.responses import FileResponse

app = FastAPI()


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
                "datalogger": 1,  # We assume this data comes from the same sensor, so we set its datalogger ID to 1
                "date": timestamp,
                "precipitation": values["precip"],
                "temperature": values["temp"],
                "humidity": values["hum"]
            }
            formatted_data.append(formatted_entry)

    # Creating a DataFrame with 'id' as the index
    df = pd.DataFrame(formatted_data)
    df['date'] = pd.to_datetime(df['date'], unit='ms')
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
    value: float


class DataRecordAggregateResponse(BaseModel):
    label: LabelField
    time_slot: datetime
    value: float


##########Business part#########

def timescale_is_wrong(since, before):
    return since < before


# Function to retrieve data from the CSV file
def get_data_from_csv(datalogger: str, before: datetime, since: datetime, span: str):
    try:
        dataframe = pd.read_csv("database.csv", index_col='id')
        dataframe['date'] = pd.to_datetime(dataframe['date'])
        filtered_df = dataframe[(dataframe['date'] >= since) & (dataframe['date'] <= before) & (
                dataframe['datalogger'] == datalogger)]

        # Convert the DataFrame to a list of DataRecordResponse objects
        result = [
            DataRecordResponse(
                label=LabelField(row['label']),
                measured_at=row['date'],
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
    df = pd.read_csv("database.csv", index_col='id', parse_dates=['date'])
except FileNotFoundError:
    df = pd.DataFrame(columns=['date', 'precip', 'temp', 'hum'])


@app.get("/api/summary", response_model=List[DataRecordAggregateResponse])
async def api_fetch_data_aggregates(datalogger: str, before: datetime = Query(datetime.now()),
                                    since: datetime = Query(datetime.now()), span: str | None = "raw"):
    return get_data_from_csv(datalogger, before, since, span)


@app.get("/api/data", response_model=List[DataRecordResponse])
async def api_fetch_data_raw(datalogger: str, before: datetime = Query(datetime.now()),
                             since: datetime = Query(datetime.now())):
    return get_data_from_csv(datalogger, before, since, "raw")
