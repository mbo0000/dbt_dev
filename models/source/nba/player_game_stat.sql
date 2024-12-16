{{ config(
    materialized="view"
) }}

select 
    AST
    ,BLK
    ,DREB
    ,FG3A
    ,FG3M
    ,div0(FG3M,FG3A) as FG3_PCT
    ,FGA
    ,FGM
    ,div0(FGM,FGA) as FG_PCT
    ,FTA
    ,FTM
    ,div0(FTM,FTA) as FT_PCT
    ,g.GAME_DATE
    ,GAME_ID
    ,ID
    ,MATCHUP
    ,MIN
    ,OREB
    ,PF
    ,PLAYER_ID
    ,PLUS_MINUS
    ,PTS
    ,REB
    ,SEASON_ID
    ,STL
    ,TOV
    ,VIDEO_AVAILABLE
    ,WL
    , try_to_number(right(SEASON_ID, 4)) as year
from {{source('raw', 'player_game_stat')}} as p
    left join {{ref('games')}} as g on p.game_id = p.game_id