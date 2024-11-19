{{ config(
    materialized="view"
) }}

select
    ts.player
    , year
    , t.full_name as team_name
    , t.ABBREVIATION as team
    , round(avg(PTS),1) as point_pg
    , round(avg(REB),1) as rebound_pg
    , round(avg(AST),1) as assist_pg
    , sum(FG3M) as three_point_made
    , round(div0(sum(FG3M), sum(FG3A)::decimal) * 100, 1) as three_point_percentage
    , round(div0(sum(FGM) , sum(FGA)::decimal) * 100, 1) as field_goal_percentage
    , round(avg(BLK),1) as block_per_game
    , round(avg(STL),1) as steal_per_game
    , round(div0(sum(FTM) , sum(FTA)::decimal) * 100, 1) as free_throw_percentage
from {{ref('player_game_stat')}} as ps
    join {{ref('team_roster')}} as ts 
        on ps.player_id = ts.player_id and ps.year = ts.season
    join {{ref('teams')}} as t on ts.TEAMID = t.id
group by 1,2,3,4