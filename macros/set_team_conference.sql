{% macro set_team_conference(team) -%}
    
    {% set east_team = ('Atlanta Hawks', 'Boston Celtics', 'Brooklyn Nets', 'Charlotte Hornets', 'Chicago Bulls', 'Cleveland Cavaliers', 'Detroit Pistons', 'Indiana Pacers', 'Miami Heat', 'Milwaukee Bucks', 'New York Knicks', 'Orlando Magic', 'Philadelphia 76ers', 'Toronto Raptors', 'Washington Wizards') %}
    {% set west_team = ('Dallas Mavericks', 'Denver Nuggets', 'Golden State Warriors', 'Houston Rockets', 'Los Angeles Clippers', 'Los Angeles Lakers', 'Memphis Grizzlies', 'Minnesota Timberwolves', 'New Orleans Pelicans', 'Oklahoma City Thunder', 'Phoenix Suns', 'Portland Trail Blazers', 'Sacramento Kings', 'San Antonio Spurs', 'Utah Jazz') %}

    case 
        when {{team}} in {{east_team}} then 'East'
        when {{team}} in {{west_team}} then 'West' 
    end

{%- endmacro %}