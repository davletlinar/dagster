from numpy import partition
from sqlmodel import Field, SQLModel
from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID

class Variables(SQLModel, table=True):
    '''variables table model'''
    id: Optional[int] = Field(default=None, primary_key=True)
    key: str
    value: str


class Calls(SQLModel, table=True):
    '''megafon_calls table model'''
    
    location: int
    uid: str = Field(default=None, primary_key=True)
    calltype: str
    wait: int
    start: datetime
    duration: int
    status: str
    client: str
    diversion: str
    destination: str
    agent: str
    username: str
    record: str
    transcription: str
    
    
class Transcriptions(SQLModel, table=True):
    '''transcriptions table model'''
    uid: str = Field(default=None, primary_key=True)
    text: str


class Sales(SQLModel, table=True):
    '''sales table model'''
    id: Optional[int] = Field(default=None, primary_key=True)
    datetime: datetime
    store: int = Field(foreign_key="stores.id")
    item: int = Field(foreign_key="items.id")
    color: Optional[int] = Field(default=None, foreign_key="colors.id")
    size: Optional[int] = Field(default=None, foreign_key="sizes.id")
    kids: bool
    quantity: int
    cost: int
    

class Sales_Receipts(SQLModel, table=True):
    '''sales table model'''
    id: Optional[int] = Field(default=None, primary_key=True)
    datetime: datetime
    store: int = Field(foreign_key="stores.id")
    item: int = Field(foreign_key="items.id")
    color: Optional[int] = Field(default=None, foreign_key="colors.id")
    size: Optional[int] = Field(default=None, foreign_key="sizes.id")
    kids: bool
    quantity: int
    cost: int


class Stores(SQLModel, table=True):
    '''stores table model'''
    id: Optional[int] = Field(default=None, primary_key=True)
    store: str
    open: bool = Field(default=True)

class Items(SQLModel, table=True):
    '''items table model'''
    id: Optional[int] = Field(default=None, primary_key=True)
    item: str

class Colors(SQLModel, table=True):
    '''colors table model'''
    id: Optional[int] = Field(default=None, primary_key=True)
    color: str

class Sizes(SQLModel, table=True):
    '''sizes table model'''
    id: Optional[int] = Field(default=None, primary_key=True)
    size: str

class Receipts(SQLModel, table=True):
    '''receipts table model'''
    date: datetime = Field(primary_key=True)
    store: int = Field(foreign_key="stores.id")
    receipts_all: int
    receipts_returns: int
    receipts_sales: int

class Leftovers(SQLModel, table=True):
    '''leftovers table model'''
    date: datetime = Field(primary_key=True)
    store: int = Field(foreign_key="stores.id")
    leftovers: int
    
class Missing_Stores(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    store: int = Field(foreign_key="stores.id")
    prttn: str

class Money(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    doc_guid: int
    doc_date: datetime
    doc_name: str
    org_name: int = Field(foreign_key="org_names.id")
    money_type: int = Field(foreign_key="money_types.id")
    cash_account_name: int = Field(foreign_key="cash_account_names.id")
    accont_number: int = Field(foreign_key="account_numbers.id")
    flow: int = Field(foreign_key="flows.id")
    operation: int = Field(foreign_key="operations.id")
    contragent: int = Field(foreign_key="contragents.id")
    sum_in: Decimal
    sum_out: Decimal

class OrgNames(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    org_name: str

class MoneyTypes(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    money_type: str

class CashAccountNames(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    cash_account_name: str

class AccountNumbers(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    account_number: str

class Flows(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    flow: str

class Operations(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    operation: str

class Contragents(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    contragent: str