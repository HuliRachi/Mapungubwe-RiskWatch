create table dim_date(
    date_id     int          not null,
    date        TIMESTAMP    not null,
    month       int          not null,
    season      varchar(20)  not null,
    is_weekend  boolean      not null,

    constraint pk_dim_date primary key (date_id)
);

create table dim_ranger(
    ranger_id               int         not null,
    ranger_name             varchar(50) not null,
    years_of_experience     int         not null,

    constraint pk_dim_ranger primary key (ranger_id)

);

create table dim_zone(
    zone_id                     int         not null,
    zone_name                   varchar(50) not null,
    habitat_type                varchar(50) not null,
    distance_from_gate          int         not null,
    historical_poaching_count   int         not null,

    constraint pk_dim_zone primary key (zone_id)

);

create table fact_incidents(
    incident_id         int         not null,
    date_id             int         not null,
    date                TIMESTAMP   not null,
    zone_id             int         not null,
    incident_type       varchar(50) not null,
    animals_involved    int         not null,
    outcome             varchar(20) not null,
    animals             varchar(20) not null,

    constraint pk_fact_incidents primary key (incident_id)

);

create table fact_patrol(
    patrol_id       int             not null,
    date_id         int             not null,
    date            TIMESTAMP       not null,
    zone_id         int             not null,
    ranger_id       int             not null,
    hours_patrolled int             not null,
    sightings_count int             not null,
    risk_score      decimal(10,2)   not null,

    constraint pk_fact_patrol primary key (patrol_id)

);