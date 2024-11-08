{{ config(
    materialized="view"
) }}

select * from {{source('raw', 'teams')}} limit 10;