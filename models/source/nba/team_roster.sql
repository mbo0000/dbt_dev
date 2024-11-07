{{ config(
    materialized="view"
) }}

select
    n.AGE
    , n.BIRTH_DATE
    , n.EXP
    , n.HEIGHT
    , n.HOW_ACQUIRED
    , n.ID
    , n.LEAGUEID
    , n.NICKNAME
    , n.NUM
    , n.PLAYER
    , n.PLAYER_ID
    , n.PLAYER_SLUG
    , n.POSITION
    , n.SCHOOL
    , n.SEASON
    , n.TEAMID
    , n.WEIGHT
    , CURRENT_TIMESTAMP() AS SYNC_TIME
from {{source('raw', 'team_roster')}} as n