{{ config(
    materialized="view"
) }}

select 
    *
    , try_to_number(right(SEASON_ID, 4)) as year
from {{source('raw', 'player_game_stat')}}