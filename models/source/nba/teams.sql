{{ config(
    materialized="view"
) }}

select 
    *
    , {{set_team_conference('FULL_NAME')}} as team_conference
from {{source('raw', 'teams')}}