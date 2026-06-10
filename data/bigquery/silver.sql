-- dim_date truncate and load
create table if not exists `project-a2ce378b-71f9-4087-95b.silver_dataset.dim_date`(
    date_id         int64,
    date            timestamp,
    month           int64,
    season          string,
    is_weekend      boolean,
    is_quarantined  boolean
);
truncate table `project-a2ce378b-71f9-4087-95b.silver_dataset.dim_date`;

insert into `project-a2ce378b-71f9-4087-95b.silver_dataset.dim_date`
select date_id, date, month, season, is_weekend,
    case
        when date_id is null then True 
        else False
    end as is_quarantined
from(
    select date_id, date, month, season, is_weekend from  `project-a2ce378b-71f9-4087-95b.bronze_dataset.dim_date`
);

-- dim_ranger truncate and load
create table if not exists `project-a2ce378b-71f9-4087-95b.silver_dataset.dim_ranger`(
    ranger_id           int64,
    ranger_name         string,
    years_of_experience int64,
    is_quarantined      boolean
);
truncate table `project-a2ce378b-71f9-4087-95b.silver_dataset.dim_ranger`;

insert into `project-a2ce378b-71f9-4087-95b.silver_dataset.dim_ranger`
select ranger_id, ranger_name, years_of_experience,
    case 
        when ranger_id is null then True
        else False
    end as is_quarantined
from(
    select ranger_id, ranger_name, years_of_experience from `project-a2ce378b-71f9-4087-95b.bronze_dataset.dim_ranger`
);
    

-- dim_zone truncate and load
create table if not exists `project-a2ce378b-71f9-4087-95b.silver_dataset.dim_zone`(
    zone_id                     int64,
    zone_name                   string,
    habitat_type                string,
    distance_from_gate          int64,
    historical_poaching_count   int64,
    is_quarantined              boolean
);
truncate table `project-a2ce378b-71f9-4087-95b.silver_dataset.dim_zone`;

insert into `project-a2ce378b-71f9-4087-95b.silver_dataset.dim_zone`
select zone_id, zone_name, habitat_type, distance_from_gate, historical_poaching_count,
    case
        when zone_id is null then True
        else False
    end as is_quarantined
from (
    select zone_id, zone_name, habitat_type, distance_from_gate, historical_poaching_count
    from `project-a2ce378b-71f9-4087-95b.bronze_dataset.dim_zone`
);

-- fact_incidents truncate and apply scd type 2
create table if not exists `project-a2ce378b-71f9-4087-95b.silver_dataset.fact_incidents`(
    incident_id         int64,
    date_id             int64,
    date                timestamp,
    zone_id             int64,
    incident_type       string,
    animals_involved    int64,
    outcome             string,
    animal              string,
    is_quarantined      boolean,
    is_current          boolean,
    InsertedDate        timestamp,
    ModifiedDate        timestamp
);
create or replace table `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_check` as
select incident_id, date_id, date, zone_id, incident_type, animals_involved, outcome,
        animal, 
    case
        when incident_id is null or date_id is null then True
        else False
    end as is_quarantined
from(
    select incident_id, date_id, date, zone_id, incident_type, animals_involved, outcome,
        animal from `project-a2ce378b-71f9-4087-95b.bronze_dataset.fact_incidents`
);

merge into `project-a2ce378b-71f9-4087-95b.silver_dataset.fact_incidents` as target
using `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_check` as source
on target.incident_id = source.incident_id
and target.is_current = True

when matched and (
    target.date_id <> source.date_id OR
    target.date <> source.date OR
    target.zone_id <> source.zone_id OR
    target.incident_type <> source.incident_type OR
    target.animals_involved <> source.animals_involved OR
    target.outcome <> source.outcome OR
    target.animal <> source.animal OR
    target.is_quarantined <> source.is_quarantined
)
then update set 
    target.is_current = False,
    target.ModifiedDate = CURRENT_TIMESTAMP()

when not matched
then insert(
    incident_id,
    date_id,
    date,
    zone_id,
    incident_type,
    animals_involved,
    outcome,
    animal,
    is_quarantined,
    is_current,
    InsertedDate,
    ModifiedDate
)
values(
    source.incident_id,
    source.date_id,
    source.date,
    source.zone_id,
    source.incident_type,
    source.animals_involved,
    source.outcome,
    source.animal,
    source.is_quarantined,
    True,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);
drop table if exists `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks`;

-- fact_patrol truncate and apply scd type 2
create table if not exists `project-a2ce378b-71f9-4087-95b.silver_dataset.fact_patrol`(
    patrol_id       int64,
    date_id         int64,
    date            timestamp,
    zone_id         int64,
    ranger_id       int64,
    hours_patrolled int64,
    sightings_count int64,
    risk_score      float64,
    is_quarantined  boolean,
    is_current      boolean,
    InsertedDate    timestamp,
    ModifiedDate    timestamp
);

create or replace table `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks` as
select patrol_id,date_id, date, zone_id, ranger_id, hours_patrolled, sightings_count,
risk_score, 
    case
        when patrol_id is null and date_id is null then True
        else False
    end as is_quarantined
from(
    select patrol_id,date_id, date, zone_id, ranger_id, hours_patrolled, sightings_count,
risk_score from `project-a2ce378b-71f9-4087-95b.bronze_dataset.fact_patrol`
);

merge into `project-a2ce378b-71f9-4087-95b.silver_dataset.fact_patrol` as target
using `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks` as source
on target.patrol_id = source.patrol_id
and target.is_current = True

when matched and (
    target.date_id <> source.date_id OR
    target.date <> source.date OR
    target.zone_id <> source.zone_id OR
    target.ranger_id <> source.ranger_id OR
    target.hours_patrolled <> source.hours_patrolled OR
    target.sightings_count <> source.sightings_count OR
    target.risk_score <> source.risk_score OR
    target.is_quarantined <> source.is_quarantined 
)
then update set
    target.is_current = False,
    target.ModifiedDate = CURRENT_TIMESTAMP()

when not matched
then insert(
    patrol_id,
    date_id,
    date,
    zone_id,
    ranger_id,
    hours_patrolled,
    sightings_count,
    risk_score,
    is_quarantined,
    is_current,
    InsertedDate,
    ModifiedDate
)
values(
    source.patrol_id,
    source.date_id,
    source.date,
    source.zone_id,
    source.ranger_id,
    source.hours_patrolled,
    source.sightings_count,
    source.risk_score,
    source.is_quarantined,
    True,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);

drop table if exists `project-a2ce378b-71f9-4087-95b.silver_dataset.quality_checks`;
