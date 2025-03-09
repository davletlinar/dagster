from typing import Sequence
from polars import DataFrame
import polars as pl


def convert_to_dict(sql_response: Sequence) -> dict:
    '''convert sql response to dictionary'''
    return dict(sql_response)


def replace_stores(df: DataFrame) -> DataFrame:
    # replace values in column 'store'
    mapping = {'CULT ТРЦ Сигма': 'Сигма',
               'CULT ТЦ ЦУМ': 'ЦУМ',
               'CULT Казань': 'Тандем',
               'CULT ТРЦ Петровский': 'Петровский',
               'CULT СПБ': 'ЗК-Спб'}
    df = df.with_columns(replaced=pl.col('store').replace(mapping)).drop(['store']).rename({'replaced': 'store'})
    
    return df

if __name__ == "__main__":
    pass