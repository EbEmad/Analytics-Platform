\c arsenalfc_analytics

DROP VIEW IF EXISTS  metrics.player_advanced_stats CASCADE;

CREATE OR REPLACE VIEW metrics.player_advanced_stats AS
SELECT
    s.team AS team_name,
    s.player_name,
    s.season,
    COUNT(DISTINCT s.match_url) as matches_played,
    COUNT(*) as total_shots,
    COUNT(*) FILTER (WHERE s.result = 'Goal') as goals,
    ROUND(SUM(s.xg)::numeric, 2) as total_xg,
    ROUND(AVG(s.xg)::numeric, 3) as avg_xg_per_shot,
    ROUND((COUNT(*) FILTER (WHERE s.result = 'Goal')::numeric / NULLIF(COUNT(*), 0) * 100), 1) as conversion_pct,
    COUNT(*) FILTER (WHERE s.result IN ('Goal', 'SavedShot', 'ShotOnPost')) as shots_on_target,
    ROUND((COUNT(*) FILTER (WHERE s.result IN ('Goal', 'SavedShot', 'ShotOnPost'))::numeric / NULLIF(COUNT(*), 0) * 100), 1) as shot_accuracy_pct,
    COUNT(*) FILTER (WHERE s.result = 'MissedShots') as missed_shots,
    COUNT(*) FILTER (WHERE s.result = 'BlockedShot') as blocked_shots,
    COUNT(*) FILTER (WHERE s.result = 'SavedShot') as saved_shots,
    COUNT(*) FILTER (WHERE s.xg >= 0.35) as big_chances,
    COUNT(*) FILTER (WHERE s.xg >= 0.35 AND s.result = 'Goal') as big_chances_scored,
    ROUND((COUNT(*) FILTER (WHERE s.xg >= 0.35 AND s.result = 'Goal')::numeric / NULLIF(COUNT(*) FILTER (WHERE s.xg >= 0.35), 0) * 100), 1) as big_chance_conversion_pct,
    COUNT(*) FILTER (WHERE s.x_coord >= 0.83) as box_shots,
    COUNT(*) FILTER (WHERE s.x_coord < 0.83) as outside_box_shots,
    ROUND(AVG((1 - s.x_coord) * 105)::numeric, 1) as avg_shot_distance,
    COUNT(*) FILTER (WHERE s.shot_type = 'RightFoot') as right_foot_shots,
    COUNT(*) FILTER (WHERE s.shot_type = 'RightFoot' AND s.result = 'Goal') as right_foot_goals,
    COUNT(*) FILTER (WHERE s.shot_type = 'LeftFoot') as left_foot_shots,
    COUNT(*) FILTER (WHERE s.shot_type = 'LeftFoot' AND s.result = 'Goal') as left_foot_goals,
    COUNT(*) FILTER (WHERE s.shot_type = 'Head') as headers,
    COUNT(*) FILTER (WHERE s.shot_type = 'Head' AND s.result = 'Goal') as header_goals,
    COUNT(*) FILTER (WHERE s.situation = 'OpenPlay') as open_play_shots,
    COUNT(*) FILTER (WHERE s.situation = 'OpenPlay' AND s.result = 'Goal') as open_play_goals,
    COUNT(*) FILTER (WHERE s.situation = 'FromCorner') as corner_shots,
    COUNT(*) FILTER (WHERE s.situation IN ('SetPiece', 'DirectFreekick')) as set_piece_shots,
    COUNT(*) FILTER (WHERE s.situation = 'Penalty') as penalties_taken,
    COUNT(*) FILTER (WHERE s.situation = 'Penalty' AND s.result = 'Goal') as penalties_scored,
    COUNT(*) FILTER (WHERE s.assisted_by IS NOT NULL AND s.assisted_by != '') as assists,
    ROUND((COUNT(*) FILTER (WHERE s.result = 'Goal') - SUM(s.xg))::numeric, 2) as xg_overperformance,
    ROUND((COUNT(*)::numeric / NULLIF(COUNT(DISTINCT s.match_url), 0)), 2) as shots_per_match,
    ROUND((COUNT(*) FILTER (WHERE s.result = 'Goal')::numeric / NULLIF(COUNT(DISTINCT s.match_url), 0)), 2) as goals_per_match,
    ROUND((SUM(s.xg)::numeric / NULLIF(COUNT(DISTINCT s.match_url), 0)), 2) as xg_per_match

FROM silver.shot_events s
WHERE s.player_name IS NOT NULL
GROUP BY s.team, s.player_name, s.season
ORDER BY total_xg DESC;

DROP VIEW IF EXISTS metrics.tactical_analysis CASCADE;
CREATE OR REPLACE VIEW metrics.tactical_analysis AS
SELECT
    s.team AS team_name,
    s.season,
    COUNT(*) FILTER (WHERE s.minute BETWEEN 0 AND 15) as arsenal_shots_0_15,
    COUNT(*) FILTER (WHERE s.minute BETWEEN 16 AND 30) as arsenal_shots_16_30,
    COUNT(*) FILTER (WHERE s.minute BETWEEN 31 AND 45) as arsenal_shots_31_45,
    COUNT(*) FILTER (WHERE s.minute BETWEEN 46 AND 60) as arsenal_shots_46_60,
    COUNT(*) FILTER (WHERE s.minute BETWEEN 61 AND 75) as arsenal_shots_61_75,
    COUNT(*) FILTER (WHERE s.minute > 75) as arsenal_shots_76_90,
    COUNT(*) FILTER (WHERE s.minute BETWEEN 0 AND 15 AND s.result = 'Goal') as arsenal_goals_0_15,
    COUNT(*) FILTER (WHERE s.minute BETWEEN 16 AND 30 AND s.result = 'Goal') as arsenal_goals_16_30,
    COUNT(*) FILTER (WHERE s.minute BETWEEN 31 AND 45 AND s.result = 'Goal') as arsenal_goals_31_45,
    COUNT(*) FILTER (WHERE s.minute BETWEEN 46 AND 60 AND s.result = 'Goal') as arsenal_goals_46_60,
    COUNT(*) FILTER (WHERE s.minute BETWEEN 61 AND 75 AND s.result = 'Goal') as arsenal_goals_61_75,
    COUNT(*) FILTER (WHERE s.minute > 75 AND s.result = 'Goal') as arsenal_goals_76_90,
    COUNT(*) FILTER (WHERE s.last_action = 'Pass') as shots_from_pass,
    COUNT(*) FILTER (WHERE s.last_action = 'Dribble') as shots_from_dribble,
    COUNT(*) FILTER (WHERE s.last_action = 'Rebound') as shots_from_rebound,
    COUNT(*) FILTER (WHERE s.last_action = 'Chipped') as shots_from_chip,
    COUNT(*) FILTER (WHERE s.last_action = 'Cross') as shots_from_cross,
    COUNT(*) FILTER (WHERE s.situation = 'OpenPlay') as open_play_total,
    COUNT(*) FILTER (WHERE s.situation = 'OpenPlay' AND s.result = 'Goal') as open_play_goals,
    ROUND(SUM(s.xg) FILTER (WHERE s.situation = 'OpenPlay')::numeric, 2) as open_play_xg,
    COUNT(*) FILTER (WHERE s.situation = 'FromCorner') as corner_total,
    COUNT(*) FILTER (WHERE s.situation = 'FromCorner' AND s.result = 'Goal') as corner_goals,
    ROUND(SUM(s.xg) FILTER (WHERE s.situation = 'FromCorner')::numeric, 2) as corner_xg,
    COUNT(*) FILTER (WHERE s.situation IN ('SetPiece', 'DirectFreekick')) as set_piece_total,
    COUNT(*) FILTER (WHERE s.situation IN ('SetPiece', 'DirectFreekick') AND s.result = 'Goal') as set_piece_goals,
    ROUND(SUM(s.xg) FILTER (WHERE s.situation IN ('SetPiece', 'DirectFreekick'))::numeric, 2) as set_piece_xg,
    COUNT(*) FILTER (WHERE s.situation = 'Penalty') as penalty_total,
    COUNT(*) FILTER (WHERE s.situation = 'Penalty' AND s.result = 'Goal') as penalty_goals,
    
    COUNT(*) FILTER (WHERE s.xg >= 0.35) as big_chances_created,
    COUNT(*) FILTER (WHERE s.xg >= 0.35 AND s.result = 'Goal') as big_chances_converted

FROM silver.shot_events s
GROUP BY s.team, s.season
ORDER BY s.season DESC, s.team;


DROP VIEW IF EXISTS metrics.player_xt_stats CASCADE;

CREATE OR REPLACE VIEW metrics.player_xt_stats AS
SELECT
    s.team AS team_name,
    s.player_name,
    s.position_category,
    s.season,
    COUNT(*) as total_shots,
    COUNT(*) FILTER (WHERE s.result = 'Goal') as goals,
    ROUND(SUM(metrics.calculate_xt_value(s.x_coord, s.y_coord))::numeric, 3) as total_xt,
    ROUND(AVG(metrics.calculate_xt_value(s.x_coord, s.y_coord))::numeric, 4) as avg_xt_per_shot,
    ROUND(MAX(metrics.calculate_xt_value(s.x_coord, s.y_coord))::numeric, 4) as max_xt_shot,
    ROUND(SUM(s.xg)::numeric, 2) as total_xg,
    ROUND(AVG(s.xg)::numeric, 3) as avg_xg_per_shot,
    COUNT(*) FILTER (WHERE metrics.calculate_xt_value(s.x_coord, s.y_coord) >= 0.1) as high_threat_shots,
    ROUND((COUNT(*) FILTER (WHERE metrics.calculate_xt_value(s.x_coord, s.y_coord) >= 0.1)::numeric / NULLIF(COUNT(*), 0) * 100), 1) as high_threat_pct,
    ROUND((SUM(metrics.calculate_xt_value(s.x_coord, s.y_coord)) / NULLIF(SUM(s.xg), 0))::numeric, 2) as xt_efficiency
FROM silver.shot_events s
WHERE s.player_name IS NOT NULL
GROUP BY s.team, s.player_name, s.position_category, s.season
ORDER BY total_xt DESC;


CREATE OR REPLACE VIEW metrics.match_shots_detail AS
SELECT
    s.match_url,
    s.match_date,
    s.season,
    s.home_team,
    s.away_team,
    s.home_goals,
    s.away_goals,
    s.home_xg,
    s.away_xg,
    s.player_name,
    s.player_id,
    s.team,
    s.minute,
    s.result,
    s.situation,
    s.shot_type,
    s.x_coord,
    s.y_coord,
    s.xg,
    s.assisted_by,
    s.last_action,
    s.position_category
FROM silver.shot_events s;


DROP VIEW IF EXISTS metrics.match_advanced_stats CASCADE;
CREATE OR REPLACE VIEW metrics.match_advanced_stats AS
SELECT
    m.match_url,
    m.match_date,
    m.season,
    m.team_name,
    m.opponent,
    m.venue,
    m.result,
    m.team_goals AS arsenal_goals,
    m.opponent_goals,
    m.team_xg AS arsenal_xg,
    m.opponent_xg,
    COUNT(*) FILTER (WHERE s.team = m.team_name) AS arsenal_shots,
    COUNT(*) FILTER (WHERE s.team != m.team_name) AS opponent_shots,
    COUNT(*) FILTER (WHERE s.team = m.team_name AND s.result IN ('Goal', 'SavedShot', 'ShotOnPost')) AS arsenal_shots_on_target,
    COUNT(*) FILTER (WHERE s.team != m.team_name AND s.result IN ('Goal', 'SavedShot', 'ShotOnPost')) AS opponent_shots_on_target,
    ROUND(COUNT(*) FILTER (WHERE s.team = m.team_name AND s.result IN ('Goal', 'SavedShot', 'ShotOnPost'))::DECIMAL /
          NULLIF(COUNT(*) FILTER (WHERE s.team = m.team_name), 0) * 100, 1) AS arsenal_shot_accuracy_pct,
    COUNT(*) FILTER (WHERE s.team = m.team_name AND s.xg >= 0.35) AS arsenal_big_chances,
    COUNT(*) FILTER (WHERE s.team = m.team_name AND s.xg >= 0.35 AND s.result = 'Goal') AS arsenal_big_chances_scored,
    COUNT(*) FILTER (WHERE s.team != m.team_name AND s.xg >= 0.35) AS opponent_big_chances,
    COUNT(*) FILTER (WHERE s.team = m.team_name AND s.x_coord >= 0.83) AS arsenal_box_shots,
    COUNT(*) FILTER (WHERE s.team = m.team_name AND s.x_coord < 0.83) AS arsenal_outside_box_shots,
    COUNT(*) FILTER (WHERE s.team = m.team_name AND s.situation = 'OpenPlay') AS arsenal_open_play_shots,
    COUNT(*) FILTER (WHERE s.team = m.team_name AND s.situation = 'FromCorner') AS arsenal_corner_shots,
    COUNT(*) FILTER (WHERE s.team = m.team_name AND s.situation = 'SetPiece') AS arsenal_set_piece_shots,
    COUNT(*) FILTER (WHERE s.team = m.team_name AND s.situation = 'Penalty') AS arsenal_penalties,
    COUNT(*) FILTER (WHERE s.team = m.team_name AND s.minute <= 45) AS arsenal_first_half_shots,
    ROUND(SUM(s.xg) FILTER (WHERE s.team = m.team_name AND s.minute <= 45)::numeric, 2) AS arsenal_first_half_xg,
    COUNT(*) FILTER (WHERE s.team = m.team_name AND s.minute > 45) AS arsenal_second_half_shots,
    ROUND(SUM(s.xg) FILTER (WHERE s.team = m.team_name AND s.minute > 45)::numeric, 2) AS arsenal_second_half_xg,
    ROUND(AVG(s.xg) FILTER (WHERE s.team = m.team_name)::numeric, 3) AS arsenal_avg_shot_xg,
    ROUND(AVG(s.xg) FILTER (WHERE s.team != m.team_name)::numeric, 3) AS opponent_avg_shot_xg
FROM metrics.team_matches m
LEFT JOIN silver.shot_events s ON m.match_url = s.match_url
GROUP BY m.match_url, m.match_date, m.season, m.team_name, m.opponent, m.venue, m.result,
         m.team_goals, m.opponent_goals, m.team_xg, m.opponent_xg
ORDER BY m.match_date DESC;


GRANT SELECT ON metrics.player_advanced_stats TO analytics_user;
GRANT SELECT ON metrics.tactical_analysis TO analytics_user;
GRANT SELECT ON metrics.player_xt_stats TO analytics_user;
GRANT SELECT ON metrics.match_shots_detail TO analytics_user;
GRANT SELECT ON metrics.match_advanced_stats TO analytics_user;

INSERT INTO gold.dim_team (team_name)
SELECT DISTINCT team_name
FROM bronze.match_reference
WHERE team_name IS NOT NULL
  AND team_name NOT IN (SELECT team_name FROM gold.dim_team)
ON CONFLICT DO NOTHING;

