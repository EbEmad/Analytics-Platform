\c arsenalfc_analytics

CREATE OR REPLACE VIEW silver.shot_events AS
WITH lineup_positions AS(
    SELECT 
        l.match_url,
        lineup_player->>'player_name' AS player_name,
        lineup_player->>'position' AS position,
        lineup_player->>'position_category' AS position_category,
        lineup_player->>'team_side' AS team_side
    FROM bronze.fbref_lineups l,
    jsonb_array_elements(
        COALESCE(l.raw_lineups->'home_lineup', '[]'::jsonb) ||
        COALESCE(l.raw_lineups->'away_lineup', '[]'::jsonb)
    ) AS lineup_player
)


SELECT 
    
    r.match_id,
    r.match_url,
    ref.match_date,
    ref.home_team,
    ref.away_team,
    ref.season,

    (r.raw_shots->>'home_xg')::DECIMAL(5,2) AS home_xg,
    (r.raw_shots->>'away_xg')::DECIMAL(5,2) AS away_xg,
    (r.raw_shots->>'home_goals')::INTEGER AS home_goals,
    (r.raw_shots->>'away_goals')::INTEGER AS away_goals,

    shot->>'player_name' AS player_name,
    shot->>'player_id' AS player_id,
    shot->>'h_a' AS home_away,
    CASE
        WHEN shot->>'h_a' = 'h' THEN ref.home_team
        ELSE ref.away_team
    END AS team,

    pos.position,
    pos.position_category,

    COALESCE((shot->>'minute')::INTEGER, 0) AS minute,
    shot->>'result' AS result,
    shot->>'situation' AS situation,
    shot->>'shot_type' AS shot_type,

    COALESCE((shot->>'x_coord')::DECIMAL(5,2), 0) AS x_coord,
    COALESCE((shot->>'y_coord')::DECIMAL(5,2), 0) AS y_coord,
    COALESCE((shot->>'xg')::DECIMAL(5,4), 0) AS xg,

    shot->>'assisted_by' AS assisted_by,
    shot->>'last_action' AS last_action,

    r.scraped_at

    FROM bronze.understat_raw r 
    INNER JOIN bronze.match_reference ref ON r.match_url = ref.match_url
    CROSS JOIN jsonb_array_elements(r.raw_shots->'shots') AS shot
    LEFT JOIN lineup_position pos ON(
        r.match_url = pos.match_url
        AND shot->>'player_name' = pos.player_name
        AND CASE WHEN shot->>'h_a' = 'h' THEN 'home' ELSE 'away' END = pos.team_side
    )
    WHERE shot->>'player_name' IS NOT NULL;




    