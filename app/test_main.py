from datetime import datetime

from fastapi.testclient import TestClient
import pandas as pd

from main import app, get_data_from_csv, DataRecordResponse, DataRecordAggregateResponse, map_aggregate_data, \
    map_raw_data, LabelField

client = TestClient(app)


##########BUSINESS TESTS###########

def test_get_data_from_csv():
    # Create a sample DataFrame for testing
    data = {
        'datalogger': [1, 1, 1],
        'date': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'precip': [1.0, 2.0, 3.0],
        'temp': [20.0, 25.0, 30.0],
        'hum': [50.0, 60.0, 70.0],
    }
    df = pd.DataFrame(data)

    # Test with raw span
    result_raw = get_data_from_csv(datalogger="1", before=datetime(2023, 1, 4), since=datetime(2023, 1, 1), span="raw")
    assert all(isinstance(record, DataRecordResponse) for record in result_raw)

    # Test with aggregate span
    result_aggregate = get_data_from_csv(datalogger="1", before=datetime(2023, 1, 4), since=datetime(2023, 1, 1),
                                         span="D")
    assert all(isinstance(record, DataRecordAggregateResponse) for record in result_aggregate)


def test_map_raw_data():
    # Create a sample DataFrame and the expected result for testing
    data = {
        'date': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'precip': [1.0, 2.0, 3.0],
        'temp': [20.0, 25.0, 30.0],
        'hum': [50.0, 60.0, 70.0],
    }
    expected_result = [
        DataRecordResponse(label=LabelField.PRECIPITATION, measured_at=datetime(2023, 1, 1), value=1.0),
        DataRecordResponse(label=LabelField.TEMPERATURE, measured_at=datetime(2023, 1, 1), value=20.0),
        DataRecordResponse(label=LabelField.HUMIDITY, measured_at=datetime(2023, 1, 1), value=50.0),
        DataRecordResponse(label=LabelField.PRECIPITATION, measured_at=datetime(2023, 1, 2), value=2.0),
        DataRecordResponse(label=LabelField.TEMPERATURE, measured_at=datetime(2023, 1, 2), value=25.0),
        DataRecordResponse(label=LabelField.HUMIDITY, measured_at=datetime(2023, 1, 2), value=60.0),
        DataRecordResponse(label=LabelField.PRECIPITATION, measured_at=datetime(2023, 1, 3), value=3.0),
        DataRecordResponse(label=LabelField.TEMPERATURE, measured_at=datetime(2023, 1, 3), value=30.0),
        DataRecordResponse(label=LabelField.HUMIDITY, measured_at=datetime(2023, 1, 3), value=70.0),
    ]
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])

    # Test map_raw_data function
    result = map_raw_data(df)
    assert all(isinstance(record, DataRecordResponse) for record in result)
    assert result == expected_result


def test_map_aggregate_data():
    # Create a sample DataFrame for testing
    data = {
        'date': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'precip': [1.0, 2.0, 3.0],
        'temp': [20.0, 25.0, 30.0],
        'hum': [50.0, 60.0, 70.0],
    }
    expected_result = [
        DataRecordAggregateResponse(label='precip', time_slot=datetime(2023, 1, 1), value=1.0),
        DataRecordAggregateResponse(label='temp', time_slot=datetime(2023, 1, 1), value=20.0),
        DataRecordAggregateResponse(label='hum', time_slot=datetime(2023, 1, 1), value=50.0),
        DataRecordAggregateResponse(label='precip', time_slot=datetime(2023, 1, 2), value=2.0),
        DataRecordAggregateResponse(label='temp', time_slot=datetime(2023, 1, 2), value=25.0),
        DataRecordAggregateResponse(label='hum', time_slot=datetime(2023, 1, 2), value=60.0),
        DataRecordAggregateResponse(label='precip', time_slot=datetime(2023, 1, 3), value=3.0),
        DataRecordAggregateResponse(label='temp', time_slot=datetime(2023, 1, 3), value=30.0),
        DataRecordAggregateResponse(label='hum', time_slot=datetime(2023, 1, 3), value=70.0),
    ]
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df.set_index(df['date'])
    # Convert the index to DatetimeIndex
    df.index = pd.to_datetime(df.index)

    # Test map_aggregate_data function
    result = map_aggregate_data(df, span='D')
    assert all(isinstance(record, DataRecordAggregateResponse) for record in result)
    assert result == expected_result

##########API TESTS############
def test_read_index():
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_api_fetch_data_aggregates():  # This test fails because I'm not able to properly format the data as a list of DataRecordAggregateResponse :/
    # All parameters are given
    response = client.get("/api/summary?datalogger=1&before=2022-06-30T00:01:00&since=2022-06-22T00:00:00&span=D")

    assert response.status_code == 200
    data = response.json()
    assert data == get_data_from_csv("1", datetime.strptime("2022-06-30T00:01:00", "%Y-%m-%dT%H:%M:%S"),
                                     datetime.strptime("2022-06-22T00:00:00", "%Y-%m-%dT%H:%M:%S"), "D")

    # All optional parameters are omitted
    response = client.get("/api/summary?datalogger=1")
    assert response.status_code == 200
    data = response.json()
    assert data == get_data_from_csv("1", datetime.now(), None, "raw")


def test_api_fetch_data_raw():
    # All parameters are given
    response = client.get("/api/data?datalogger=1&before=2022-01-01T00:01:00&since=2022-01-01T00:00:00")

    assert response.status_code == 200
    data = response.json()
    assert data == get_data_from_csv("1", datetime.strptime("2022-01-01T00:01:00", "%Y-%m-%dT%H:%M:%S"),
                                     datetime.strptime("2022-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"), "raw")

    # All optional parameters are omitted is already tested through "test_api_fetch_data_aggregates()"


# Run the tests
if __name__ == "__main__":
    import pytest

    pytest.main(["-sv", "test_app.py"])
