{{ config(materialized='table') }}

SELECT
    {{ const_as_string(elementary.get_elementary_package_version()) }} as dbt_pkg_version
