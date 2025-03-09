-- create database Role
create database "role";

-- create database dagster
create database dagster;

-- create database metabase
create database metabase;

-- connect to role database
\c role

-- create Items table
create table
    if not exists items (id serial primary key, item varchar(255) not null);

-- create Colors table
create table
    if not exists colors (
        id serial primary key,
        color varchar(255) not null
    );

-- create Sizes table
create table
    if not exists sizes (id serial primary key, size varchar(255) not null);

-- create Stores table
create table
    if not exists stores (
        id serial primary key,
        store varchar(255) not null,
        open boolean not null default true
    );

-- create Sales table
create table
    if not exists sales (
        id serial primary key,
        datetime timestamp not null,
        store int not null references stores (id),
        item int not null references items (id),
        color int references colors (id),
        size int references sizes (id),
        kids boolean not null,
        quantity int not null,
        cost int not null
    );

-- create SalesReceipts table
create table
    if not exists sales_receipts (
        id serial primary key,
        datetime timestamp not null,
        store int not null references stores (id),
        item int not null references items (id),
        color int references colors (id),
        size int references sizes (id),
        kids boolean not null,
        quantity int not null,
        cost int not null
    );

-- create Sales view
create view
    sales_view as
select
    s.id,
    s.datetime,
    stores.store,
    items.item,
    colors.color,
    sizes.size,
    s.kids,
    s.quantity,
    s.cost
from
    sales as s
    left join stores on s.store = stores.id
    left join items on s.item = items.id
    left join colors on s.color = colors.id
    left join sizes on s.size = sizes.id

-- create Receipts table
create table
    if not exists receipts (
        date date not null,
        store int not null references stores (id),
        receipts_all int not null,
        receipts_returns int not null,
        receipts_sales int not null,
        primary key (date, store)
    );

-- create Receipts view
create view
    receipts_view as
select
    r.date,
    s.store,
    r.receipts_all,
    r.receipts_returns,
    r.receipts_sales
from
    receipts r
    left join stores s on r.store = s.id;

-- create Leftovers table
create table
    if not exists leftovers (
        date date not null,
        store int not null references stores (id),
        leftovers int not null,
        primary key (date, store)
    );

-- create Leftovers view
create view
    leftovers_view as
select l.date,
    s.store,
    l.leftovers,
    extract(dow from l.date) as day_of_week
from leftovers l
    left join stores s on l.store = s.id;

-- create Plan_monthly table
create table
    if not exists plan_monthly (
        date date not null,
        store varchar(255) not null,
        plan int not null,
        primary key (date, store)
    );

-- create table missed_stores
create table if not exists missed_stores (
    id serial primary key,
    store int not null references stores (id),
    partition datetime not null
)