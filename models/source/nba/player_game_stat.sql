{{ config(
    materialized="view"
) }}

select 
    p.AST
    ,p.BLK
    ,p.DREB
    ,p.FG3A
    ,p.FG3M
    ,div0(p.FG3M,p.FG3A) as FG3_PCT
    ,p.FGA
    ,p.FGM
    ,div0(p.FGM,p.FGA) as FG_PCT
    ,p.FTA
    ,p.FTM
    ,div0(p.FTM,p.FTA) as FT_PCT
    ,g.GAME_DATE
    ,p.GAME_ID
    ,p.ID
    ,p.MATCHUP
    ,p.MIN
    ,p.OREB
    ,p.PF
    ,p.PLAYER_ID
    ,p.PLUS_MINUS
    ,p.PTS
    ,p.REB
    ,p.SEASON_ID
    ,p.STL
    ,p.TOV
    ,p.VIDEO_AVAILABLE
    ,p.WL
    , try_to_number(right(p.SEASON_ID, 4)) as year
from {{source('raw', 'player_game_stat')}} as p
    left join (select distinct game_id, game_date from {{source('raw', 'games')}}) as g on p.game_id = g.game_id
