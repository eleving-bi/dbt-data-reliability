{{
  config(
    materialized = 'view',
    bind=False
  )
}}


with information_schema_columns as (
    select
        lower(database_name) as database_name,
        lower(schema_name) as schema_name,
        lower(table_name) as table_name,
        lower(column_name) as name,
        data_type
    from {{ ref("information_schema_columns") }}
),

dbt_columns as (
    select
        lower(database_name) as database_name,
        lower(schema_name) as schema_name,
        lower(table_name) as table_name,
        lower(name) as name,
        description
    from {{ ref("dbt_columns") }}
)

SELECT
    isc.database_name,
    isc.schema_name,
    isc.table_name,
    isc.name,
    isc.data_type,
    dbc.description
FROM
    information_schema_columns isc
LEFT JOIN dbt_columns dbc ON
    isc.database_name = dbc.database_name
    AND isc.schema_name = dbc.schema_name
    AND isc.table_name = dbc.table_name
    AND isc.name = dbc.name
