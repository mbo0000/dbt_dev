{{ config(
    materialized="view"
) }}

select * from {{source('raw', 'player_game_stat')}}