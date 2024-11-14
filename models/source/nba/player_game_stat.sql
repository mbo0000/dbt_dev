{{ config(
    materialized="view"
) }}

select 
    *
    , right('SEASON_ID', 4) as year
from {{source('raw', 'player_game_stat')}}