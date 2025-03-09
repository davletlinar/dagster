from typing import Any, Optional
import pendulum
import requests
from dagster import (
    AutoMaterializePolicy,
    asset,
    multi_asset,
    AssetSpec,
    AssetKey,
    AssetIn,
    AssetOut,
    op,
    TimeWindowPartitionsDefinition,
    Output,
    AssetExecutionContext,
    build_schedule_from_partitioned_job,
    define_asset_job,
    get_dagster_logger,
    AssetSelection,
    AutomationConditionSensorDefinition,
    )
from sqlmodel import Session, select
from .hooks import notify_telegram_on_failure, send_telegram_message
from .classes import Calls, Variables, Transcriptions
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import gspread
import polars as pl
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import os

load_dotenv()

APIS_CONFIG = {
    'ekb': {
        'get_url': f'{os.environ['EKB_URL']}',
        'api_key': f'{os.environ['EKB_API_KEY']}',
        'spreadsheet_id': f'{os.environ['EKB_SPREADSHEET_ID']}',
        'location_id': 1
    }
    # 'ufa': {
    #     'get_url': 'https://vats455535.megapbx.ru/crmapi/v1/history/json',
    #     'api_key': 'c4a29ef3-6292-4f85-810a-58a85f559f6b',
    #     'spreadsheet_id': '16yw2YVtegm5aYrL9yCIeOoDMzQSLPSn_Q5MSD0V8-7U',
    #     'location_id': 2
    # },
    # 'chelyabinsk': {
    #     'get_url': 'https://vats455535.megapbx.ru/crmapi/v1/history/json',
    #     'api_key': 'c4a29ef3-6292-4f85-810a-58a85f559f6b',
    #     'spreadsheet_id': '1fwWoLiG00jpvQKnfAXsHONJDk8D-8UOyjSk9tYokMN8',
    #     'location_id': 3
    # },
    # 'perm': {
    #     'get_url': 'https://vats455535.megapbx.ru/crmapi/v1/history/json',
    #     'api_key': 'c4a29ef3-6292-4f85-810a-58a85f559f6b',
    #     'spreadsheet_id': '1Ueci-bqzGmNP0nrBWOvxJvff7mhhdeyLboWZGsSie4M',
    #     'location_id': 4
    # },
    # 'kazan': {
    #     'get_url': 'https://vats455535.megapbx.ru/crmapi/v1/history/json',
    #     'api_key': 'c4a29ef3-6292-4f85-810a-58a85f559f6b',
    #     'spreadsheet_id': '1e2Ltux9Ztd92YLeMS92mz8Ly8YYFmgSk7BHkXknaGwQ',
    #     'location_id': 5
    # }
}

RANGE_NAME = 'Sheet1!A1'  # Adjust to your target range
# create a variable with a path to your service account credentials
credentials_path = 'repos/megafon_vats_data/albert-434120-5200ac4e8219.json'

logger = get_dagster_logger(name='megafon_logger')


def create_new_tab(title: str, spreadsheet_id: str) -> Any | HttpError:
    """
    Adds a new tab (sheet) with the specified title to an existing Google Spreadsheet.
    """
    try:
        # Set up the credentials and authorize the service account
        # scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        # creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        # client = build("sheets", "v4", credentials=creds)

        # Prepare the request body to add a new sheet
        body = {
            "requests": [{
                "addSheet": {
                    "properties": {
                        "title": title,
                        "gridProperties": {
                            "rowCount": 1000,
                            "columnCount": 16
                        }
                    }
                }
            }]
        }
        # create client
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        client = build("sheets", "v4", credentials=creds)
        # Execute the request to add a new sheet/tab to the existing spreadsheet
        response = client.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

        print(f"New sheet '{title}' created successfully.")
        return response
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error


def update_current_month(conn: Any, current_month: str, location_id: int) -> None:
    '''Update current month'''
    with Session(conn) as session:
        row = session.exec(select(Variables).where(Variables.key == f'current_month_{location_id}')).one()
        row.value = current_month
        session.add(row)
        session.commit()
        session.refresh(row)


def get_tabname(first_row: pl.DataFrame) -> str:
    '''Get tab name from first row'''
    
    months_rus = {'1': 'Январь', '2': 'Февраль', '3': 'Март', '4': 'Апрель',
                 '5': 'Май', '6': 'Июнь', '7': 'Июль', '8': 'Август',
                 '9': 'Сентябрь', '10': 'Октябрь', '11': 'Ноябрь', '12': 'Декабрь'}
    
    # Extract the date from the first row
    date_str = first_row['start'][0]
    logger.info(date_str)
    
    # Convert the string to a datetime object
    date_obj = pendulum.parse(date_str)
    logger.info(date_obj)

    # create tuple with year and month
    year, month = date_obj.year, date_obj.month
    
    # translate formatted date to russian
    month_rus_str = months_rus[str(month)]
        
    return f'{year} {month_rus_str}'


def check_current_month(conn, data: str, location_id: int) -> bool:
    '''Check if data is from current month. If month is changed, return True'''
    incoming_date = pendulum.parse(data)
    
    with Session(conn) as session:
        current_month_str = session.exec(select(Variables.value).where(Variables.key == f'current_month_{location_id}')).one()
    current_date = pendulum.parse(current_month_str)
    
    return incoming_date.month != current_date.month


partitions_megafon = TimeWindowPartitionsDefinition(
    start=pendulum.datetime(2025, 1, 23, 0, 0, 0, tz="UTC"),
    cron_schedule="*/30 * * * *", # every 30 minutes
    fmt="%Y-%m-%d %H:%M:%S",
)


def insert_transcription(
    link: str,
    uid: str
    ) -> str:
    
    try:
        response = requests.post(
            'http://gigaam_api:1488/transcribe',
            json={"uid": uid, "record": link}
        )
        text = response.json()['transcription']

        return text

    except Exception as e:
        logger.error(e)
        return 'Internal error'


def transform_megafon_calls(
    response_json: list[dict]
    ) -> pl.DataFrame:
    '''Transform calls data from Megafon API'''
    
    # convert list of dictionaries to dataframe
    df = pl.from_dicts(response_json)
    
    # add column 'duration' with value None if there is no 'duration' column
    if 'duration' not in df.columns:
        df = df.with_columns(pl.lit(None).alias('duration'))

    # add column 'record' with value None if there is no 'record' column
    if 'record' not in df.columns:
        df = df.with_columns(pl.lit(None).alias('record'))
        
    # change columns order
    df = df.select(['start', 'uid', 'type', 'status', 'client', 'diversion', \
        'destination', 'user', 'user_name', 'wait', 'duration', 'record'])
    
    # replace null values in duration with 0
    df = df.with_columns(pl.when(pl.col('duration').is_null())
    .then(0)
    .otherwise(pl.col('duration'))
    .alias('duration'))
    
    # sort the dataframe by start
    df = df.sort(by='start')
    
    # replace T and Z with empty strings
    df = df.with_columns(pl.col('start').str.replace('T', ' ').str.replace('Z', ''))
    
    # replace empty 'destination' with 'null'
    df = df.with_columns(pl.when(pl.col('destination') == '')
    .then(pl.lit('null'))
    .otherwise(pl.col('destination'))
    .alias('destination'))
    
    return df


@multi_asset(
    name='MegafonCallsFromAPI',
    group_name='megafon',
    partitions_def=partitions_megafon,
    outs={
        f'{location}_api': AssetOut() for location in APIS_CONFIG.keys()
        },
    )
def megafon_calls(context: AssetExecutionContext):
    '''Get data from Megafon API'''
    
    results = []
    
    for location, config in APIS_CONFIG.items():
        time_window = context.partition_time_window
        
        start_time = time_window.start - pendulum.duration(minutes=30)
        start_time = start_time.strftime('%Y%m%dT%H%M%SZ')
        
        end_time = time_window.end - pendulum.duration(minutes=30, seconds=1)
        end_time = end_time.strftime('%Y%m%dT%H%M%SZ')
        
        logger.info(f'Get data from Megafon API for {location} from {start_time} to {end_time}')
        
        headers = {
            'X-API-KEY': config['api_key'],
        }
        params = {
            'start': start_time,
            'end': end_time
        }
        get_url = config['get_url']
        response = requests.get(get_url, headers=headers, params=params)
        response.raise_for_status()
        
        if response.json() is None:
            results.append(
                Output(
                value=None,
                metadata={
                    'start_time': start_time,
                    'end_time': end_time,
                    'first_row': 'No records for this period',
                },
                )
            )
        else:
            # create first row sample
            first_row = response.json()[0]
            
            # create datframe for transformation
            df = transform_megafon_calls(response.json())
            
            # create transcription column
            df = df.with_columns(
                transcription=pl.Series(
                    [insert_transcription(record, uid) if record is not None else None 
                    for record, uid in zip(df['record'], df['uid'])]
                )
            )

            results.append(
                Output(
                value=df,
                metadata={
                    'start_time': start_time,
                    'end_time': end_time,
                    'first_row': first_row,
                    'rows processed': len(df)
                },
                )
            )
            
    return tuple(results)


@multi_asset(
    name='MegafonCallsToDB',
    group_name='megafon',
    partitions_def=partitions_megafon,
    ins={
        f'{location}_api': AssetIn(key=AssetKey(f'{location}_api')) for location in APIS_CONFIG.keys()
    },
    specs=[
        AssetSpec(
            key=AssetKey(f'{location}_db'),
            partitions_def=partitions_megafon,
            deps=[AssetKey(f'{location}_api')],
        ) for location in APIS_CONFIG.keys()
    ],
    required_resource_keys={'megafon_postgres'},
    )
def insert_megafon_calls_db(
    context: AssetExecutionContext,
    **MegafonCallsFromAPI: pl.DataFrame | None
    ):
    '''Push data to postgres'''
    
    for location, config in APIS_CONFIG.items():
        df = MegafonCallsFromAPI[f'{location}_api']
        
        # in case a tuple is returned
        if isinstance(df, tuple):
            df = df[0]
            
        # Extract the actual DataFrame from the Output object
        if hasattr(df, 'value'):
            df = df.value
            
        # in case dataframe is empty
        if df is None:
            logger.info('No data to insert')
            return None
        
        conn = context.resources.megafon_postgres
        try:
            with Session(conn) as session:
                for row in df.rows(named=True):
                    table_row = Calls(
                        location=config['location_id'],
                        uid=row['uid'],
                        calltype=row['type'],
                        wait=row['wait'],
                        start=row['start'],
                        duration=row['duration'],
                        status=row['status'],
                        client=row['client'],
                        diversion=row['diversion'],
                        destination=row['destination'],
                        agent=row['user'],
                        username=row['user_name'],
                        record=row['record'],
                        transcription=row['transcription'],
                    )
                    session.add(table_row)
                session.commit()
        except Exception as e:
            logger.error(f'Error: {e}')
            # send_telegram_message(message=f'Error: {e}')
            notify_telegram_on_failure(context)
            raise e


@multi_asset(
    name='MegafonCallsToGoogleSheets',
    group_name='megafon',
    partitions_def=partitions_megafon,
    ins={
        f'{location}_api': AssetIn(key=AssetKey(f'{location}_api')) for location in APIS_CONFIG.keys()
    },
    specs=[
        AssetSpec(
            key=AssetKey(f'{location}_google_sheets'),
            partitions_def=partitions_megafon,
            deps=[AssetKey(f'{location}_api')]
        ) for location in APIS_CONFIG.keys()
    ],
    required_resource_keys={'megafon_postgres'},
)
def insert_megafon_calls_google_sheets(
    context: AssetExecutionContext,
    **MegafonCallsFromAPI: pl.DataFrame | None
    ):
    '''Insert the retrieved data into Google Sheets'''
    
    for location, config in APIS_CONFIG.items():
        df = MegafonCallsFromAPI[f'{location}_api']
        # in case a tuple is returned
        if isinstance(df, tuple):
            df = df[0]
            
        # Extract the actual DataFrame from the Output object
        if hasattr(df, 'value'):
            df = df.value
            
        # in case dataframe is empty
        if df is None:
            logger.info('No data to insert')
            return None
        
        # Setup the Google Sheets API client
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        client = gspread.authorize(creds)
        
        # check whether month is changed
        conn = context.resources.megafon_postgres
        if check_current_month(conn, df['start'][0], config['location_id']):
            current_month_str = df['start'][0].split()[0]
            update_current_month(conn, current_month_str, config['location_id'])
            create_new_tab(get_tabname(df[0]), spreadsheet_id=config['spreadsheet_id'])
        
        # Open the spreadsheet and insert the data
        tabname = get_tabname(df[0])
        sheet = client.open_by_key(config['spreadsheet_id']).worksheet(tabname)
        sheet.append_rows(df.rows())
        

megafon_job = define_asset_job(
    name='megafon_job',
    selection=AssetSelection.groups('megafon'),
    partitions_def=partitions_megafon,
    description='get data from megafon virtual ATS and insert it into database',
    )


megafon_schedule = build_schedule_from_partitioned_job(
    megafon_job,
    tags={'group': 'megafon'},
    )

# Define asset backfill sensor
megafon_automation_sensor = AutomationConditionSensorDefinition(
    name='MegafonAutomationSensor',
    target=AssetSelection.groups('megafon'),
    minimum_interval_seconds=86400,
    )

megafon_assets = [
    megafon_calls,
    insert_megafon_calls_db,
    insert_megafon_calls_google_sheets,
    ]

# [2024-10-01 00:00:00...2024-10-03 00:00:00]