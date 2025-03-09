-- create database
create database megafon;

-- create Calls table with timestampz datatype
create table if not exists calls (
    location int not null references locations(id),
    uid varchar(255),
    calltype varchar(255) not null,
    wait int not null,
    start timestamp not null,
    duration int,
    status varchar(255) not null,
    client varchar(255) not null,
    diversion varchar(255) not null,
    destination varchar(255) not null,
    agent varchar(255) not null,
    username varchar(255) not null,
    record varchar(255),
    transcription text,
    primary key (location, uid)
);

-- create Locations table
create table
    if not exists locations (
        id serial primary key,
        location varchar(255) not null
    );

-- create Variables table
create table
    if not exists variables (
        id serial primary key,
        key varchar(255) not null,
        value varchar(255) not null
    );

-- insert data into Locations table
insert into locations (location) values
    ('ekb'),
    ('ufa'),
    ('chelyabinsk'),
    ('perm')
    ('kazan'),

-- insert current month into Variables table
insert into variables (key, value) values
    ('current_month_1', '2025-01-01'),
    ('current_month_2', '2025-01-01'),
    ('current_month_3', '2025-01-01'),
    ('current_month_4', '2025-01-01'),
    ('current_month_5', '2025-01-01'),