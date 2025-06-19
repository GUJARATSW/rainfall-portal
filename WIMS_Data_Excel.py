import requests
import pandas as pd
from datetime import datetime, date
import threading
import ujson  # Ultra-fast JSON encoder/decoder

# Configuration: list of Data Type codes to fetch and include
DATA_TYPE_CODES = ['GPR', 'GPC', 'GHT', 'MS3']

# Global lock to synchronize file operations
t_file_lock = threading.Lock()


def fetch_and_process_data():
    # Current date and time
    today = date.today()
    current_date = today.strftime('%Y-%m-%d')
    current_time = datetime.now().strftime('%H:%M:%S')

    # API endpoint and payload
    url = "https://india-water.gov.in/web-api/getDateWiseDataforAgency"
    start_date = f"{current_date} 00:00:00"
    end_date = f"{current_date} {current_time}"
    data_type_code_str = ",".join(f"'{code}'" for code in DATA_TYPE_CODES)
    payload = {
        "stationCode": "'all'",
        "projectName": "'NHP'",
        "dataTypeCode": data_type_code_str,
        "agencyCode": "46",
        "startDate": start_date,
        "endDate": end_date
    }

    # Fetch data
    resp = requests.post(url, json=payload, verify=False)
    if resp.status_code != 200:
        print(f"[{datetime.now()}] Fetch failed: {resp.status_code} - {resp.text}")
        return

    try:
        data = pd.json_normalize(resp.json())
    except ValueError as e:
        print(f"[{datetime.now()}] JSON parse error: {e}")
        return

    # Parse datetime columns
    if not data.empty:
        for col in list(data.columns):
            if 'datetime' in col.lower() or pd.api.types.is_datetime64_any_dtype(data[col]):
                data['Date'] = pd.to_datetime(data[col]).dt.strftime('%d-%m-%Y')
                data['Time'] = pd.to_datetime(data[col]).dt.time
                data.drop(columns=[col], inplace=True)

    # Rename columns
    data.rename(columns={
        'stationCode': 'Station Code',
        'agencyName': 'Agency Name',
        'dataTypeCode': 'Data Type',
        'dataValue': 'Data Value',
        'projectName': 'Project Name'
    }, inplace=True)
    data['Data Value'] = pd.to_numeric(data['Data Value'], errors='coerce').fillna('')

    # Load station metadata
    stations = pd.read_excel('AWS_ARG_AWLR.xlsx')
    stations.columns = stations.columns.str.strip()
    stations = stations[[
        'Station Code','Station Name','District','Taluka',
        'Latitude','Longitude','Station Type','RTDAS Type'
    ]]

    # Merge API data with stations
    merged = pd.merge(data, stations, on='Station Code', how='left')
    timestamp = today.strftime('%d-%m-%Y') + f" at {current_time}"
    merged['Date Time'] = timestamp

    # Ensure columns exist for each requested code
    for code in DATA_TYPE_CODES:
        merged[code] = merged.get(code, '')

    # Pivot to wide format
    pivoted = merged.pivot_table(
        index=[
            'Station Code','Station Name','District','Taluka',
            'Date','Time','Date Time','Project Name','Agency Name',
            'Station Type','RTDAS Type'
        ],
        columns='Data Type',
        values='Data Value',
        aggfunc='first'
    ).reset_index()

    # Add lat/lon back
    final = pd.merge(
        pivoted,
        stations[['Station Code','Latitude','Longitude']],
        on='Station Code', how='left'
    )

    # Include any missing stations
    missing = stations[~stations['Station Code'].isin(data.get('Station Code', []))]
    if not missing.empty:
        missing = missing.copy()
        missing['Date'] = today.strftime('%d-%m-%Y')
        missing['Time'] = ''
        missing['Date Time'] = timestamp
        missing[['Project Name','Agency Name']] = ''
        for code in DATA_TYPE_CODES:
            missing[code] = ''
        # Drop latitude/longitude from missing before concatenation
        missing = missing.drop(columns=['Latitude','Longitude'])
        final = pd.concat([final, missing], ignore_index=True)

    # Save to Excel
    out_file = 'WIMS_Gujarat_SWDC_Test.xlsx'
    with t_file_lock:
        final.to_excel(out_file, index=False)
        print(f"[{datetime.now()}] Written to {out_file}")


if __name__ == '__main__':
    fetch_and_process_data()
