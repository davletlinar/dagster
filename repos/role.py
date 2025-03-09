from datetime import datetime
import warnings
from requests import get
import polars as pl
from polars import DataFrame
import pendulum as pd
from sqlmodel import Session, select
from .classes import Items, Missing_Stores, Sales_Receipts, Stores, Sales, Colors, Sizes, Leftovers, Receipts
import numpy as np
from .tools import convert_to_dict, replace_stores
from dotenv import load_dotenv
import os
from .hooks import send_telegram_message

from dagster import (
    ExperimentalWarning,
    asset,
    define_asset_job,
    build_schedule_from_partitioned_job,
    TimeWindowPartitionsDefinition,
    Output,
    AssetExecutionContext,
    get_dagster_logger,
    AssetSelection,
    RetryPolicy,
    materialize_to_memory,
    )

# Define logger
logger = get_dagster_logger('role_logger')
# Load environment variables
load_dotenv()
# Filter warnings
warnings.filterwarnings("ignore", category=ExperimentalWarning)
# Load environment variables
ROLE_API_USERNAME = os.environ['ROLE_API_USERNAME']
ROLE_API_PASSWORD = os.environ['ROLE_API_PASSWORD']
HOST_PORT = os.environ['ROLE_HOST_PORT']

# Define constants
no_data = {'first_record': 'No data for this period'}
# Define retry policy
role_retry_policy = RetryPolicy(max_retries=0, delay=60)
# Define partitions
partitions_role = TimeWindowPartitionsDefinition(
    start=pd.datetime(2024, 1, 1, 0, 0, 0, tz="UTC"), # start from 2024-01-01 00:00:00
    cron_schedule="0 2 * * *",
    fmt="%Y-%m-%d %H:%M:%S",
)


def get_response(link: str) -> dict:
    response = get(link, auth=(ROLE_API_USERNAME, ROLE_API_PASSWORD), verify=False)
    return response.json()


def get_period_start(context: AssetExecutionContext) -> str:
    period_start = context.partition_time_window.start.strftime('%Y-%m-%d')
    return period_start


def notify_no_data(context: AssetExecutionContext) -> None:
    send_telegram_message(f'No data for period {context.partition_time_window.start}')
    return None


def check_stores(context: AssetExecutionContext, session, df: DataFrame, sales: bool = False) -> None:
    '''check if all stores returned data'''
    # create a set of stores from db
    current_stores = set(session.exec(select(Stores.store).where(Stores.open == True)).all())
    # create a set of stores from dataframe
    import_stores = set(df['store'].unique())
    # find missing stores
    missing_stores = current_stores - import_stores
    if missing_stores:
        # create markdown message
        missing_stores_str = ', '.join(list(current_stores - import_stores))
        message = f'''
            No data for period {context.partition_time_window.start.strftime('%Y-%m-%d')}  
            __Stores:__ {missing_stores_str}  
            __Asset:__ {context.asset_key.to_string()}
        '''
        send_telegram_message(message=message)
        
        if sales:
            conn = context.resources.role_postgres
            with Session(conn) as session:
                store_ids = convert_to_dict(session.exec(select(Stores.store, Stores.id)).all())
                for store in list(missing_stores):
                    session.add(Missing_Stores(store=store_ids[store],
                                               prttn=context.partition_key))
                session.commit()
    return None

  
@asset(
    name='get_role_sales',
    group_name='role',
    partitions_def=partitions_role,
    retry_policy=role_retry_policy,
    owners=['davletka@gmail.com'],
    description='get json data from server',
    op_tags={'group': 'role', 'subgroup': 'sales', 'type': 'extract'}
)
def get_role_sales(context: AssetExecutionContext) -> Output[dict | None]:
    
    period_start = get_period_start(context)
    link = f"http://{HOST_PORT}/unf/hs/metabase/get_sales_report?period_start={period_start}&period_end={period_start}"
    response = get_response(link)

    logger.info(response['errorCode'])
    logger.info(response['errorText'])
    logger.info(f'✅ Sales data received: {period_start}')
    
    if not response['data']:
        notify_no_data(context)
        return Output(None, metadata=no_data)
    
    return Output(response, metadata={'first_record': response['data'][0]})


@asset(
    name='get_role_receipts',
    group_name='role',
    partitions_def=partitions_role,
    retry_policy=role_retry_policy,
    owners=['davletka@gmail.com'],
    description='get receits json data from server',
    op_tags={'group': 'role', 'subgroup': 'receipts', 'type': 'extract'}
)
def get_role_receipts(context: AssetExecutionContext) -> Output[dict | None]:
    period_start = get_period_start(context)
    link = f"http://{HOST_PORT}/unf/hs/metabase/get_cheques?period_start={period_start}&period_end={period_start}"
    response = get_response(link)

    logger.info(response['errorCode'])
    logger.info(response['errorText'])
    logger.info(f'✅ Receits data received: {period_start}')
    
    if not response['data']:
        notify_no_data(context)
        return Output(None, metadata=no_data)
    
    return Output(response, metadata={'first_record': response['data'][0]})


@asset(
    name='get_role_leftovers',
    group_name='role',
    partitions_def=partitions_role,
    retry_policy=role_retry_policy,
    owners=['davletka@gmail.com'],
    description='get json data from server',
    op_tags={'group': 'role', 'subgroup': 'leftovers', 'type': 'extract'}
)
def get_role_leftovers(context: AssetExecutionContext) -> Output[dict | None]:
    '''get leftovers json from server'''
    
    period_start = get_period_start(context)
    link = f'http://{HOST_PORT}/unf/hs/metabase/get_stocks?period={period_start}'
    response = get_response(link)

    if not response['data']:
        notify_no_data(context)
        return Output(None, metadata=no_data)
    
    return Output(response, metadata={'first_record': response['data'][0]})


@asset(
    name='transform_role_sales',
    group_name='role',
    partitions_def=partitions_role,
    owners=['davletka@gmail.com'],
    description='convert json response to polars dataframe',
    op_tags={'group': 'role', 'subgroup': 'sales', 'type': 'transform'}
)
def transform_role_sales(
    context: AssetExecutionContext,
    get_role_sales: dict | None
    ) -> Output[DataFrame | None]:
    
    if get_role_sales is None:
        return Output(None, metadata=no_data)
    
    df = pl.DataFrame(get_role_sales["data"])
    
    # remove rows with useless stores
    df = df.filter(~pl.col('sklad').is_in(['Производство ХАБ', 'OZON FBO', 'Wildberries FBO']))
    
    # filter out rows with irrelevant items
    drop_list = ['Пакет белый', 'Пакет черный', 'Листовка кофе', 'Бирка бумажная', 'Кружка',
                 'Листовка', 'Сертификат подарочный', 'Сертификат новый на 1000',
                 'Сертификат новый на 2000', 'Сертификат новый на 3000', 'Сертификат новый на 5000']
    df = df.filter(~pl.col("tovar").is_in(drop_list))
    
    # rename columns
    df = df.rename({
        'date': 'datetime',
        'tovar': 'item',
        'sklad': 'store',
        'sum': 'cost',
        'count': 'quantity',
        'harakteristika_color': 'color',
    })
    
    # remove parentheses from 'harakteristika_size' column
    df = df.with_columns(
        pl.col('harakteristika_size').str.replace_all(r'[[:punct:]]', "").alias('harakteristika_size')
    )
    
    # remove parentheses from 'harakteristika_kid' column
    df = df.with_columns(
        pl.col('harakteristika_kid').str.replace_all(r'[[:punct:]]', "").alias('harakteristika_kid')
    )
    
    # merge colums 'harakterisitika_size' and 'harakteristika_kid'
    df = df.with_columns(
        (pl.col('harakteristika_size') + pl.col('harakteristika_kid')).alias('size')
    )
    
    # replace values in column 'store'
    df = replace_stores(df)
    
    # add new column 'kid' with True if there is 'harakteristika_kid' and False if not
    df = df.with_columns(
        pl.when(pl.col('harakteristika_kid') == '').then(False).otherwise(True).alias('kid')
    )
    
    # drop columns
    df = df.drop([
        'sotrudnik', 'sotrudnik_guid', 'sum_bez_skidki', 'harakteristika_guid',
        'tovar_guid', 'sklad_guid', 'harakteristika', 'harakteristika_kid', 'harakteristika_size',
        ])
    
    
    # convert empty color strings to null
    df = df.with_columns(
        pl.when(pl.col('color') == '').then(None).otherwise(pl.col('color')).alias('color')
    )
    
    # convert empty size strings to null
    df = df.with_columns(
        pl.when(pl.col('size') == '').then(None).otherwise(pl.col('size')).alias('size')
    )
    
    logger.info('✅ Sales data transformed to polars dataframe')
    
    return Output(df, metadata={'length': len(df)})
    

@asset(
    name='transform_role_sales_receipts',
    group_name='role',
    partitions_def=partitions_role,
    owners=['davletka@gmail.com'],
    description='convert json response to polars dataframe',
    op_tags={'group': 'role', 'subgroup': 'sales', 'type': 'transform'}
)
def transform_role_sales_receipts(
    context: AssetExecutionContext,
    get_role_receipts: dict | None
    ) -> Output[DataFrame | None]:
    
    if get_role_receipts is None:
        return Output(None, metadata=no_data)
    
    df = pl.DataFrame(get_role_receipts["data"])
    
    # remove rows with useless stores
    df = df.filter(~pl.col('sklad').is_in(['Производство ХАБ', 'OZON FBO', 'Wildberries FBO']))
    
    # filter out rows with irrelevant items
    drop_list = ['Пакет белый', 'Пакет черный', 'Листовка кофе', 'Бирка бумажная', 'Кружка',
                 'Листовка', 'Сертификат подарочный', 'Сертификат новый на 1000',
                 'Сертификат новый на 2000', 'Сертификат новый на 3000', 'Сертификат новый на 5000']
    df = df.filter(~pl.col("tovar").is_in(drop_list))
    
    # rename columns
    df = df.rename({
        'date': 'datetime',
        'tovar': 'item',
        'sklad': 'store',
        'sum': 'cost',
        'count': 'quantity',
        'harakteristika_color': 'color',
    })
    
    # remove parentheses from 'harakteristika_size' column
    df = df.with_columns(
        pl.col('harakteristika_size').str.replace_all(r'[[:punct:]]', "").alias('harakteristika_size')
    )
    
    # remove parentheses from 'harakteristika_kid' column
    df = df.with_columns(
        pl.col('harakteristika_kid').str.replace_all(r'[[:punct:]]', "").alias('harakteristika_kid')
    )
    
    # merge colums 'harakterisitika_size' and 'harakteristika_kid'
    df = df.with_columns(
        (pl.col('harakteristika_size') + pl.col('harakteristika_kid')).alias('size')
    )
    
    # replace values in column 'store'
    df = replace_stores(df)
    
    # add new column 'kid' with True if there is 'harakteristika_kid' and False if not
    df = df.with_columns(
        pl.when(pl.col('harakteristika_kid') == '').then(False).otherwise(True).alias('kid')
    )
    
    # drop columns
    df = df.drop([
        'sotrudnik', 'sotrudnik_guid', 'sum_bez_skidki', 'harakteristika_guid',
        'tovar_guid', 'sklad_guid', 'harakteristika', 'harakteristika_kid', 'harakteristika_size',
        ])
    
    
    # convert empty color strings to null
    df = df.with_columns(
        pl.when(pl.col('color') == '').then(None).otherwise(pl.col('color')).alias('color')
    )
    
    # convert empty size strings to null
    df = df.with_columns(
        pl.when(pl.col('size') == '').then(None).otherwise(pl.col('size')).alias('size')
    )
    
    logger.info('✅ Sales data transformed to polars dataframe')
    
    return Output(df, metadata={'length': len(df)})


@asset(
    name='transform_role_receipts',
    group_name='role',
    partitions_def=partitions_role,
    owners=['davletka@gmail.com'],
    description='convert receipts json response to polars dataframe',
    op_tags={'group': 'role', 'subgroup': 'receipts', 'type': 'transform'}
)
def transform_role_receipts(
    context: AssetExecutionContext,
    get_role_receipts: dict | None
    ) -> Output[DataFrame | None]:
    '''convert receipts json response to pandas dataframe'''
    
    if get_role_receipts is None:
        return Output(None, metadata=no_data)
    
    df = pl.DataFrame(get_role_receipts["data"])
    
    # filter out rows with irrelevant items
    drop_list = ['Пакет белый', 'Пакет черный', 'Листовка кофе', 'Бирка бумажная', 'Кружка',
                 'Листовка', 'Сертификат подарочный', 'Сертификат новый на 1000', 'Сертификат новый на 2000',
                 'Сертификат новый на 3000', 'Сертификат новый на 5000']

    df = df.filter(~pl.col('tovar').is_in(drop_list))
    
    # remove rows with irrelevant stores
    df = df.filter(~pl.col('sklad').is_in(['Производство ХАБ', 'OZON FBO', 'Wildberries FBO']))
    
    # drop unnecessary columns
    df = df.drop(['parent_guid', 'date', 'tovar', 'tovar_guid', 'harakteristika', 'harakteristika_guid', 'harakteristika_color',
                  'harakteristika_size', 'harakteristika_kid', 'sklad_guid', 'sum', 'sum_bez_skidki', 'sotrudnik', 'sotrudnik_guid'])
    
    # rename columns
    df = df.rename({
        'sklad': 'store',
        'count': 'quantity',
        'cheque_guid': 'receipt_guid'})
    
    # replace values in column 'store'
    df = replace_stores(df)
    
    # create dataframe with unique sale receipts
    df_receipts_all = df.select(['store', 'receipt_guid'])
    df_receipts_all = df_receipts_all.group_by('store').n_unique()
    
    # create dataframe with unique return receipts
    df_receipts_returns = df.filter(pl.col('quantity') < 0).select(['store', 'receipt_guid'])
    df_receipts_returns = df_receipts_returns.group_by('store').n_unique()
    
    # merge dataframes
    df_output = df_receipts_all.join(df_receipts_returns, how='left', on='store')\
        .rename({'receipt_guid': 'receipts_all', 'receipt_guid_right': 'receipts_returns'}).fill_null(0)
        
    # substract receipts_returns from receipts_all as new column 'receipts_sales'
    df_output = df_output.with_columns(receipts_sales=pl.col('receipts_all') - pl.col('receipts_returns'))
    
    logger.info('✅ Receipts data transformed to polars dataframe')
    
    return Output(df_output, metadata={'length': len(df_output)})


@asset(
    name='update_role_sales_fk',
    group_name='role',
    partitions_def=partitions_role,
    retry_policy=role_retry_policy,
    owners=['davletka@gmail.com'],
    description='update foreign keys in database',
    required_resource_keys={'role_postgres'},
    deps=['transform_role_leftovers', 'transform_role_receipts'],
    op_tags={'group': 'role', 'subgroup': 'sales', 'type': 'update'}
)
def update_role_sales_fk(
    context: AssetExecutionContext,
    transform_role_sales: DataFrame | None
    ) -> Output[None]:
    
    # in case dataframe is empty
    if transform_role_sales is None: return Output(None, metadata=no_data)
    
    item, color, size, store = Items, Colors, Sizes, Stores
    
    mapping = {
        'item': [Items.item, Items, item], # type: ignore
        'color': [Colors.color, Colors, color], # type: ignore
        'size': [Sizes.size, Sizes, size], # type: ignore
        'store': [Stores.store, Stores, store] # type: ignore
    }
    conn = context.resources.role_postgres
    new_categories_metadata = {}
    
    # update item id in database
    for column in mapping.keys():
        with Session(conn) as session:
            # create list of current categories
            current_categories = np.array(session.exec(select(mapping[column][0]).distinct()).all())
            logger.debug(f'current_categories: {current_categories}')
            # create list of unique items from imported dataframe and
            import_categories = transform_role_sales[column].unique().to_numpy()
            logger.debug(f'import_categories: {import_categories}')
            # filter out null values
            import_categories = import_categories[import_categories != None]
            logger.debug(f'import_categories_filtered: {import_categories}')
            # check if there are new items using set difference
            new_categories = np.setdiff1d(import_categories, current_categories)
            logger.debug(f'new_categories: {new_categories}')
            # add new categories to database if exist
            if new_categories.size > 0:
                # update metadata set
                new_categories_metadata[f'new_{column}s'] = new_categories.tolist()
                # add new items to database
                for new_category in new_categories:
                    # dynamically create object
                    obj = mapping[column][1](**{column: new_category})
                    session.add(obj)
                session.commit()
                logger.info(f"✅ New {column} ids added.")
            else:
                logger.info("😐 No new item ids.")
    return Output(None, metadata={'new_categories': new_categories_metadata})


@asset(
    name='transform_role_leftovers',
    group_name='role',
    partitions_def=partitions_role,
    retry_policy=role_retry_policy,
    owners=['davletka@gmail.com'],
    description='convert json response to pandas dataframe',
    op_tags={'group': 'role', 'subgroup': 'leftovers', 'type': 'transform'}
)
def transform_role_leftovers(
    context: AssetExecutionContext,
    get_role_leftovers: dict
    ) -> Output[DataFrame | None]:
    '''convert json response to pandas dataframe'''
    
    if get_role_leftovers is None:
        return Output(None, metadata=no_data)
    
    df = pl.DataFrame(get_role_leftovers["data"])
    
    # remove rows with irrelevant stores
    df = df.filter(~pl.col('sklad').is_in(['Производство ХАБ', 'OZON FBO', 'Wildberries FBO']))
    
    # filter out rows with irrelevant items
    drop_list = ['Пакет белый', 'Пакет черный', 'Листовка кофе', 'Бирка бумажная', 'Кружка',
                 'Листовка', 'Сертификат подарочный', 'Сертификат новый на 1000',
                 'Сертификат новый на 2000', 'Сертификат новый на 3000', 'Сертификат новый на 5000',
                 'Карта Основателя CULT OFF', 'Сертификат на 1000', 'Сертификат на 2000',
                 'Сертификат на 3000', 'Сертификат на 5000', 'веревка для бейджа', 'Лимонад']
    df = df.filter(~pl.col("tovar").is_in(drop_list))
    
    # drop irrelevant columns
    drop_list = ['tovar', 'tovar_guid', 'harakteristika', 'harakteristika_guid', 'harakteristika_color',
                 'harakteristika_size', 'harakteristika_kid', 'sklad_guid']
    
    # rename columns
    df = df.rename({
        'count': 'quantity',
        'sklad': 'store',
    })
    
    # replace values in column 'store'
    df = replace_stores(df)
    
    # find sum of column 'quantity'
    df = df.select(['store', 'quantity']).group_by('store').sum()
    logger.info('✅ Leftovers dataframe created.')
    
    return Output(df)


@asset(
    name='insert_role_leftovers',
    group_name='role',
    partitions_def=partitions_role,
    required_resource_keys={'role_postgres'},
    retry_policy=role_retry_policy,
    owners=['davletka@gmail.com'],
    description='insert leftovers data to postgres table',
    deps=['update_role_sales_fk',],
)
def insert_role_leftovers(
    context: AssetExecutionContext,
    transform_role_leftovers: DataFrame
    ) -> Output[None]:
    '''insert leftovers data to postgres table'''
    
    # in case dataframe is empty
    if transform_role_leftovers is None :
        return Output(None, metadata=no_data)
    
    # get date
    period_start = get_period_start(context)
    logger.info(f'✅ Date: {period_start}')
    
    df = pl.DataFrame(transform_role_leftovers)
    
    conn = context.resources.role_postgres
    with Session(conn) as session:
        store_ids = convert_to_dict(session.exec(select(Stores.store, Stores.id)).all())
        for row in df.rows(named=True):
            session.add(Leftovers(date=datetime.strptime(period_start, '%Y-%m-%d'),
                                  store=store_ids[row['store']],
                                  leftovers=row['quantity']))
        session.commit()
    
    logger.info('✅ Leftovers inserted.')
    
    return Output(None, metadata={'rows_processed': len(transform_role_leftovers)})


@asset(
    name='insert_role_sales',
    group_name='role',
    partitions_def=partitions_role,
    retry_policy=role_retry_policy,
    owners=['davletka@gmail.com'],
    description='insert polars dataframe into database',
    required_resource_keys={'role_postgres'},
    deps=['update_role_sales_fk'],
    op_tags={'group': 'role', 'subgroup': 'sales', 'type': 'insert'}
)
def insert_role_sales(
    context: AssetExecutionContext,
    transform_role_sales: DataFrame | None
    ) -> Output[None]:
    '''insert polars dataframe into database'''
    
    # in case dataframe is empty
    if transform_role_sales is None:
        return Output(None, metadata=no_data)
    
    conn = context.resources.role_postgres
    
    with Session(conn) as session:
        # check if all stores return data
        check_stores(context, session, transform_role_sales, sales=True)
        # make dictionary of store_ids, item_ids, color_ids, size_ids
        store_ids = convert_to_dict(session.exec(select(Stores.store, Stores.id)).all())
        item_ids = convert_to_dict(session.exec(select(Items.item, Items.id)).all())
        color_ids = convert_to_dict(session.exec(select(Colors.color, Colors.id)).all())
        size_ids = convert_to_dict(session.exec(select(Sizes.size, Sizes.id)).all())
        for row in transform_role_sales.iter_rows(named=True):
            session.add(Sales(
                datetime = row['datetime'],
                store = store_ids[row['store']],
                item = item_ids[row['item']],
                color = color_ids[row['color']] if row['color'] != None else None,
                size = size_ids[row['size']] if row['size'] != None else None,
                kids = row['kid'],
                cost = row['cost'],
                quantity = row['quantity']))
        session.commit()
    logger.info("✅ Rows inserted.")
    return Output(None, metadata={'rows_processed': len(transform_role_sales)})


@asset(
    name='insert_role_sales_receipts',
    group_name='role',
    partitions_def=partitions_role,
    retry_policy=role_retry_policy,
    owners=['davletka@gmail.com'],
    description='insert polars dataframe into database',
    required_resource_keys={'role_postgres'},
    deps=['update_role_sales_fk'],
    op_tags={'group': 'role', 'subgroup': 'sales', 'type': 'insert'}
)
def insert_role_sales_receipts(
    context: AssetExecutionContext,
    transform_role_sales_receipts: DataFrame | None
    ) -> Output[None]:
    '''insert polars dataframe into database'''
    
    # in case dataframe is empty
    if transform_role_sales_receipts is None:
        return Output(None, metadata=no_data)
    
    conn = context.resources.role_postgres
    
    with Session(conn) as session:
        # check if all stores return data
        check_stores(context, session, transform_role_sales_receipts)
        # make dictionary of store_ids, item_ids, color_ids, size_ids
        store_ids = convert_to_dict(session.exec(select(Stores.store, Stores.id)).all())
        item_ids = convert_to_dict(session.exec(select(Items.item, Items.id)).all())
        color_ids = convert_to_dict(session.exec(select(Colors.color, Colors.id)).all())
        size_ids = convert_to_dict(session.exec(select(Sizes.size, Sizes.id)).all())
        for row in transform_role_sales_receipts.iter_rows(named=True):
            session.add(Sales_Receipts(
                datetime = row['datetime'],
                store = store_ids[row['store']],
                item = item_ids[row['item']],
                color = color_ids[row['color']] if row['color'] != None else None,
                size = size_ids[row['size']] if row['size'] != None else None,
                kids = row['kid'],
                cost = row['cost'],
                quantity = row['quantity']))
        session.commit()
    logger.info("✅ Rows inserted.")
    return Output(None, metadata={'rows_processed': len(transform_role_sales_receipts)})


@asset(
    name='insert_role_receipts',
    group_name='role',
    partitions_def=partitions_role,
    retry_policy=role_retry_policy,
    owners=['davletka@gmail.com'],
    description='insert polars dataframe into database',
    required_resource_keys={'role_postgres'},
    deps=['update_role_sales_fk',],
    op_tags={'group': 'role', 'subgroup': 'receipts', 'type': 'insert'}
)
def insert_role_receipts(
    context: AssetExecutionContext,
    transform_role_receipts: DataFrame | None
    ) -> Output[None]:
    '''insert receits polars dataframe into database'''
    
    # in case dataframe is empty
    if transform_role_receipts is None:
        return Output(None, metadata=no_data)
    
    # get date
    date = context.partition_time_window.start
    conn = context.resources.role_postgres
    
    with Session(conn) as session:
        # check if all stores return data
        check_stores(context, session, transform_role_receipts)
        store_ids = convert_to_dict(session.exec(select(Stores.store, Stores.id)).all())
        for row in transform_role_receipts.iter_rows(named=True):
            session.add(Receipts(date=date,
                store=store_ids[row['store']],
                receipts_all=row['receipts_all'],
                receipts_returns=row['receipts_returns'],
                receipts_sales=row['receipts_sales']))
        session.commit()
    logger.info("✅ Receits rows inserted.")
    
    return Output(None, metadata={'rows_processed': len(transform_role_receipts)})


@asset(
    name='stores_retry',
    group_name='stores',
    description='retry getting missed stores',
    required_resource_keys={'role_postgres'},
)
def stores_retry(context: AssetExecutionContext) -> Output[None]:
    conn = context.resources.role_postgres
    with Session(conn) as session:
        missing_prttns = session.exec(select(Missing_Stores)).all()
        stores_keys = convert_to_dict(session.exec(select(Stores.id, Stores.store)).all())
        logger.info(stores_keys)
    
    # find missing dates
    missing_dates = list(set([row.prttn for row in missing_prttns]))
    logger.info(f'Missing dates: {missing_dates}')
    
    # materialize missed stores
    for date in missing_dates:
        df = materialize_to_memory(
            assets=[get_role_sales, transform_role_sales,],
            partition_key=date,
        ).asset_value('transform_role_sales')
        
        logger.info(f'Assets refreshed for date: {date}')
        
        # check which stores returned data
        stores = df['store'].unique().to_list()
        logger.info(f'Stores list: {stores}')
        for prttn in missing_prttns:
            if prttn.store in stores:
                logger.info(f'Missing data returned from: {stores_keys[prttn.store]}')
                # leave only current store
                current_store_rows = df[df['store'] == prttn.store]
                # insert current store into sales
                for row in current_store_rows.iter_rows(named=True):
                    session.add(Sales(datetime=row['datetime'],
                                    store=row['store'],
                                    item=row['item'],
                                    color=row['color'],
                                    size=row['size'],
                                    kids=row['kids'],
                                    quantity=row['quantity'],
                                    cost=row['cost']))
                # delete current store from missing stores
                missing_store_row = Missing_Stores(store=prttn.store, prttn=prttn.prttn)
                session.delete(missing_store_row)
                missing_prttns = session.exec(select(Missing_Stores)).all()
                logger.info(f'Missing partitions: {missing_prttns}')
                session.commit()
            else:
                logger.info(f'⚠️ No data returned from: {stores_keys[prttn.store]}')
        
    return Output(None, metadata={'missing_stores': None})


role_job = define_asset_job(
    name='role_job',
    selection=AssetSelection.groups('role'),
    partitions_def=partitions_role,
    description='get sales, receipts and leftovers data',
    )

role_schedule = build_schedule_from_partitioned_job(
    role_job,
    tags={'group': 'role'},
    )

stores_retry_job = define_asset_job(
    name='stores_retry_job',
    selection=AssetSelection.groups('stores'),
    description='retry getting missed stores',
    )

role_assets = [
    get_role_sales,
    get_role_receipts,
    get_role_leftovers,
    transform_role_sales,
    transform_role_sales_receipts,
    transform_role_receipts,
    transform_role_leftovers,
    update_role_sales_fk,
    insert_role_sales,
    insert_role_sales_receipts,
    insert_role_leftovers,
    insert_role_receipts,
    stores_retry
    ]