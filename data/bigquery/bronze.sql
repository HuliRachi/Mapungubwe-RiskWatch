
create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.dim_date`
options(
    format = 'JSON',
    uris = ['gs://mapungubwe-bucket/landing/mapungubwe/dim_date/*json']
);

create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.dim_ranger`
options(
    format = 'JSON',
    uris = ['gs://mapungubwe-bucket/landing/mapungubwe/dim_ranger/*json']
);

create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.dim_zone`
options(
    format = 'JSON',
    uris = ['gs://mapungubwe-bucket/landing/mapungubwe/dim_zone/*json']
);

create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.fact_incidents`
options(
    format = 'JSON',
    uris = ['gs://mapungubwe-bucket/landing/mapungubwe/fact_incidents/*json']
);

create external table if not exists `project-a2ce378b-71f9-4087-95b.bronze_dataset.fact_patrol`
options(
    format = 'JSON',
    uris = ['gs://mapungubwe-bucket/landing/mapungubwe/fact_patrol/*json']
);