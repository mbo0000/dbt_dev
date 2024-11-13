{{ config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key='id'
) }}

select
    n.CONF_COUNT
    , n.CONF_RANK
    , n.DIV_COUNT
    , n.DIV_RANK
    , n.GP
    , n.WINS
    , n.LOSSES
    , ROUND(DIV0(n.WINS,n.GP::DECIMAL), 4)    AS WIN_PCT
    , n.DREB
    , n.FG3A
    , n.FG3M
    , ROUND(DIV0(n.FG3M,n.FG3A::DECIMAL), 4)  AS FG3_PCT
    , n.FGA
    , n.FGM
    , ROUND(DIV0(n.FGM,n.FGA::DECIMAL), 4)    AS FG_PCT
    , n.FTA
    , n.FTM
    , ROUND(DIV0(n.FTM,n.FTA::DECIMAL), 4)    AS FT_PCT
    , n.ID
    , case when n.NBA_FINALS_APPEARANCE = 'N/A' 
        then null else n.NBA_FINALS_APPEARANCE 
        end                                   as NBA_FINALS_APPEARANCE
    , n.OREB
    , n.PF
    , n.PO_LOSSES
    , n.PO_WINS
    , n.PTS
    , n.PTS_RANK
    , n.REB
    , n.STL
    , n.AST
    , n.BLK
    , n.TEAM_CITY
    , n.TEAM_ID
    , n.TEAM_NAME
    , n.TOV
    , n.YEAR                                 as SEASON_ID
    , try_to_number(left(n.year, 4))         as year                                     
    , CURRENT_TIMESTAMP()                    AS SYNC_TIME
from {{source('raw', 'team_stat')}} as n

-- append to table when there are new records(id is null) OR updating existing record if value(GP column) changed
{% if is_incremental() %}
    left join {{this}} as o on n.id = o.id
where True
    and CURRENT_TIMESTAMP() > (select coalesce(max(SYNC_TIME),'1900-01-01') from {{ this }})
    and (n.GP <> o.GP or o.id is null)

{% endif%}