\c arsenalfc_analytics

DROP VIEW IF EXISTS metrics.opponent_comparison CASCADE;
DROP VIEW IF EXISTS metrics.match_advanced_stats CASCADE;
DROP VIEW IF EXISTS metrics.arsenal_matches CASCADE;
DROP VIEW IF EXISTS metrics.team_matches CASCADE;
DROP VIEW IF EXISTS metrics.player_season_stats CASCADE;

-- Base view for all team matches
CREATE OR REPLACE VIEW metrics.team_matches AS
SELECT 
    ref.match_url,
    ref.match_date,
    ref.season,
    ref.team_name,
    CASE 
        WHEN ref.team_name = ref.home_team THEN ref.away_team 
        ELSE ref.home_team 
    END AS opponent,
    CASE 
        WHEN ref.team_name = ref.home_team THEN 'h' 
        ELSE 'a' 
    END AS venue,
    CASE
        WHEN ref.team_name = ref.home_team THEN
            CASE 
                WHEN (u.raw_shots->>'home_goals')::int > (u.raw_shots->>'away_goals')::int THEN 'W'
                WHEN (u.raw_shots->>'home_goals')::int < (u.raw_shots->>'away_goals')::int THEN 'L'
                ELSE 'D'
            END
        ELSE
            CASE 
                WHEN (u.raw_shots->>'away_goals')::int > (u.raw_shots->>'home_goals')::int THEN 'W'
                WHEN (u.raw_shots->>'away_goals')::int < (u.raw_shots->>'home_goals')::int THEN 'L'
                ELSE 'D'
            END
    END AS result,
    CASE 
        WHEN ref.team_name = ref.home_team THEN (u.raw_shots->>'home_goals')::int 
        ELSE (u.raw_shots->>'away_goals')::int 
    END AS team_goals,
    CASE 
        WHEN ref.team_name = ref.home_team THEN (u.raw_shots->>'away_goals')::int 
        ELSE (u.raw_shots->>'home_goals')::int 
    END AS opponent_goals,
    CASE 
        WHEN ref.team_name = ref.home_team THEN (u.raw_shots->>'home_xg')::decimal(5,2) 
        ELSE (u.raw_shots->>'away_xg')::decimal(5,2) 
    END AS team_xg,
    CASE 
        WHEN ref.team_name = ref.home_team THEN (u.raw_shots->>'away_xg')::decimal(5,2) 
        ELSE (u.raw_shots->>'home_xg')::decimal(5,2) 
    END AS opponent_xg
FROM bronze.match_reference ref
JOIN bronze.understat_raw u ON ref.match_url = u.match_url;

-- Arsenal specific view used by other metrics
CREATE OR REPLACE VIEW metrics.arsenal_matches AS
SELECT * FROM metrics.team_matches WHERE team_name = 'Arsenal';

-- Required for RAG Chatbot
CREATE OR REPLACE VIEW metrics.player_season_stats AS
SELECT 
    player_name,
    team,
    season,
    COUNT(*) AS total_shots,
    COUNT(*) FILTER (WHERE result = 'Goal') AS goals,
    SUM(xg)::decimal(10,2) AS total_xg,
    ROUND((COUNT(*) FILTER (WHERE result = 'Goal')::decimal / NULLIF(COUNT(*), 0) * 100), 1) AS conversion_rate,
    COUNT(*) FILTER (WHERE xg >= 0.35) AS big_chances,
    COUNT(*) FILTER (WHERE xg >= 0.35 AND result = 'Goal') AS big_chances_scored,
    COUNT(DISTINCT match_url) AS matches_played
FROM silver.shot_events
GROUP BY player_name, team, season;

CREATE OR REPLACE VIEW metrics.opponent_comparison AS
SELECT
    opponent,
    COUNT(*) as matches_played,
    SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) as wins,
    SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) as draws,
    SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) as losses,
    SUM(team_goals) as goals_for,
    SUM(opponent_goals) as goals_against,
    ROUND(AVG(team_xg)::numeric, 2) as avg_xg_for,
    ROUND(AVG(opponent_xg)::numeric, 2) as avg_xg_against,
    ROUND((SUM(team_goals)::numeric / NULLIF(COUNT(*), 0)), 2) as avg_goals_per_match,
    ROUND((SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100), 1) as win_rate
FROM metrics.arsenal_matches
GROUP BY opponent
ORDER BY matches_played DESC, win_rate DESC;

CREATE OR REPLACE VIEW metrics.match_advanced_stats AS
SELECT
    m.match_date,
    m.opponent,
    m.venue,
    m.result,
    m.team_goals,
    m.opponent_goals,
    m.team_xg,
    m.opponent_xg,

    -- Shot statistics
    COUNT(CASE WHEN s.team = 'Arsenal' THEN 1 END) as shots,
    COUNT(CASE WHEN s.team = 'Arsenal' AND s.result = 'Goal' THEN 1 END) as goals,
    COUNT(CASE WHEN s.team = 'Arsenal' AND s.result IN ('Goal', 'SavedShot', 'ShotOnPost') THEN 1 END) as shots_on_target,

    -- Shot quality metrics
    ROUND(AVG(CASE WHEN s.team = 'Arsenal' THEN s.xg END)::numeric, 3) as avg_shot_xg,
    MAX(CASE WHEN s.team = 'Arsenal' THEN s.xg END) as max_shot_xg,

    -- Conversion rate
    ROUND(
        (COUNT(CASE WHEN s.team = 'Arsenal' AND s.result = 'Goal' THEN 1 END)::numeric /
         NULLIF(COUNT(CASE WHEN s.team = 'Arsenal' THEN 1 END), 0) * 100),
        1
    ) as conversion_rate,

    -- xG performance
    ROUND((m.team_goals - m.team_xg)::numeric, 2) as xg_overperformance,

    -- Big chances
    COUNT(CASE WHEN s.team = 'Arsenal' AND s.xg >= 0.35 THEN 1 END) as big_chances,
    COUNT(CASE WHEN s.team = 'Arsenal' AND s.xg >= 0.35 AND s.result = 'Goal' THEN 1 END) as big_chances_scored

FROM metrics.arsenal_matches m
LEFT JOIN metrics.match_shots_detail s
    ON m.match_date = s.match_date
    AND (s.home_team = 'Arsenal' OR s.away_team = 'Arsenal')
    AND s.team = 'Arsenal'
GROUP BY
    m.match_date,
    m.opponent,
    m.venue,
    m.result,
    m.team_goals,
    m.opponent_goals,
    m.team_xg,
    m.opponent_xg
ORDER BY m.match_date DESC;

-- Grant permissions
GRANT SELECT ON metrics.opponent_comparison TO analytics_user;
GRANT SELECT ON metrics.match_advanced_stats TO analytics_user;