{{ config(
    materialized="incremental"
    ,incremental_strategy="merge"
) }}

with stat_agg as (
    select
        concat(player_id, year) as id
        , ts.player
        , year
        , t.full_name as team_name
        , t.ABBREVIATION as team
        , count(distinct ps.game_id) as game_played 
        , sum(ps.min) as total_min_played
        , avg(ps.min) as avg_min_played
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
)

select
    *
    , CURRENT_TIMESTAMP() AS SYNC_TIME
from stat_agg as n

{% if is_incremental() %}
    left join {{this}} as o on n.id = o.id
where True
    and CURRENT_TIMESTAMP() > (select coalesce(max(SYNC_TIME),'1900-01-01') from {{ this }})
    and (n.game_played <> o.game_played or o.id is null)

{% endif%}
