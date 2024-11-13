{{ config(
    materialized="incremental",
    incremental_strategy="append"
) }}


select 
    n.AST
    , n.BLK
    , n.DREB
    , n.FG3A
    , n.FG3M
    , ROUND(DIV0(n.FG3M,n.FG3A::DECIMAL), 4)    as FG3_PCT
    , n.FGA
    , n.FGM
    , ROUND(DIV0(n.FGM,n.FGA::DECIMAL), 4)      as FG_PCT
    , n.FTA
    , n.FTM
    , ROUND(DIV0(n.FTM,n.FTA::DECIMAL), 4)      as FT_PCT
    , n.GAME_DATE
    , n.GAME_ID
    , n.MATCHUP
    , n.MIN
    , n.OREB
    , n.PF
    , n.PLUS_MINUS
    , n.PTS
    , n.REB
    , n.SEASON_ID
    , try_to_number(right(n.season_id, 4))      as year
    , n.SEASON_TYPE
    , n.STL
    , n.TEAM_ABBREVIATION
    , n.TEAM_ID
    , n.TEAM_NAME
    , n.TOV
    , n.VIDEO_AVAILABLE
    , n.WL
    , CURRENT_TIMESTAMP()                       as SYNC_TIME
from {{source('raw', 'games')}} as n

-- append to table for newly ingested games
{% if is_incremental() %}
    left join {{this}} as o on n.GAME_ID = o.GAME_ID
where True
    and CURRENT_TIMESTAMP() > (select coalesce(max(SYNC_TIME),'1900-01-01') from {{ this }})
    and o.GAME_ID is null
{% endif %}