import uvicorn
from fastapi import FastAPI
import uuid
from datetime import datetime
from typing import Optional
import requests
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from pydantic import UUID4
from sqlmodel import Field, SQLModel
import pandas as pd
import json

app = FastAPI()

##########ETL part#########
url = "http://localhost:3000/measurements"
r = requests.get(url)
if r.status_code == 200:
    data = r.json()

    # Extracting data from the JSON and converting timestamp to datetime
    formatted_data = []
    for entry in data:
        for timestamp, values in entry.items():
            formatted_entry = {
                "id": str(uuid.uuid4()),  # Generate a unique ID for each row
                "timestamp": timestamp,
                "precip": values["precip"],
                "temp": values["temp"],
                "hum": values["hum"]
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

        print("Database updated successfully.")
    except FileNotFoundError:
        # If the file doesn't exist, save the current DataFrame to the CSV file
        df.to_csv("database.csv")
        print("Database file not found. Created a new file.")
else:
    print(f"Error: {r.status_code}")

##########Business part#########




##########API part#########
@app.get("/summary")
async def root():
    return {"message": "Hello World"}


@app.get("/data")
async def get_measurements():
    return
