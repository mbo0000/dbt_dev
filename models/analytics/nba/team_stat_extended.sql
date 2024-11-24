{{ config(
    materialized="view"
) }}

<<<<<<< HEAD
-- last 10 game win lost count
with l_10_games as (
=======
-- Sort the games by date to ensure proper sequencing for rn
with games_data AS (
    SELECT
        team_id,
        team_name,
        game_date,
        game_id,
        WL,
        year,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY game_date) AS rn
    FROM
        {{ref('games')}}
)


-- last 10 game win lost count
, l_10_games as (
>>>>>>> main
    select 
        team_id, team_name, year
        , concat(
            count( distinct case when wl = 'W' then game_id end)
            , '-'
            , count( distinct case when wl = 'L' then game_id end)
        ) as l10g
    from (
        select
            team_id, game_id, wl, year, team_name
        from
<<<<<<< HEAD
            {{ref('games')}}
=======
            games_data
>>>>>>> main
        qualify row_number() over(partition by team_id, year order by game_date desc) <= 10
    ) as sq
    group by team_id, team_name, year
)

<<<<<<< HEAD
, -- Sort the games by date to ensure proper sequencing for rn
games_data AS (
    SELECT
        team_id,
        game_date,
        WL,
        year,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY game_date) AS rn
    FROM
        {{ref('games')}}
),

-- Create groups based on WL changes: Use a difference of row numbers technique to identify consecutive W or L streaks.
wl_streak_groups AS (
=======
-- Create groups based on WL changes: Use a difference of row numbers technique to identify consecutive W or L streaks.
, wl_streak_groups AS (
>>>>>>> main
    SELECT
        team_id,
        game_date,
        WL,
        year,
        rn - ROW_NUMBER() OVER (PARTITION BY team_id, WL ORDER BY game_date) AS streak_group_id
    FROM
        games_data
),

-- Calculate streak lengths
streaks AS (
    SELECT
        team_id,
        WL,
        year,
        COUNT(*) AS streak_length,
        MAX(game_date) AS last_game_date
    FROM
        wl_streak_groups
    GROUP BY
        team_id, WL, streak_group_id, year
)

--Get the current streak for each team as of the latest game date.
, current_streak as (
    SELECT
        team_id,
        WL AS current_streak_type,
        year,
        streak_length AS current_streak_length
    FROM
        streaks
<<<<<<< HEAD
    QUALIFY ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY last_game_date DESC) = 1
=======
    QUALIFY ROW_NUMBER() OVER (PARTITION BY team_id, year ORDER BY last_game_date DESC) = 1
>>>>>>> main
    ORDER BY
        team_id
)

select 
    cs.team_id
    , cs.year
    , concat(cs.current_streak_type, cs.current_streak_length) as current_streak
    , lg.l10g as last_10_games
from current_streak as cs
    left join l_10_games as lg on cs.team_id = lg.team_id and cs.year = lg.year